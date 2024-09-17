package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	lib "github.com/assist-by/autro-library"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

const (
	binanceKlineAPI = "https://api.binance.com/api/v3/klines"
	maxRetries      = 5
	retryDelay      = 5 * time.Second
	candleLimit     = 300
	fetchInterval   = 1 * time.Minute
)

var (
	kafkaBroker       string
	kafkaTopic        string
	host              string
	port              string
	registrationTopic string
	isRunning         bool
	runningMutex      sync.Mutex
	serviceCtx        context.Context
	serviceCtxCancel  context.CancelFunc
	kafkaWriter       *kafka.Writer
	writerMutex       sync.Mutex
)

func init() {
	kafkaBroker = os.Getenv("KAFKA_BROKER")
	if kafkaBroker == "" {
		kafkaBroker = "kafka:9092" // 기본값 설정
	}
	kafkaTopic = os.Getenv("KAFKA_TOPIC")
	if kafkaTopic == "" {
		kafkaTopic = "price-to-signal" // 기본값 설정
	}
	host = os.Getenv("HOST")
	if host == "" {
		host = "abprice"
	}
	port = os.Getenv("PORT")
	if port == "" {
		port = "50051"
	}
	registrationTopic = os.Getenv("REGISTRATION_TOPIC")
	if registrationTopic == "" {
		registrationTopic = "service-registration"
	}
	serviceCtx, serviceCtxCancel = context.WithCancel(context.Background())

}

func initKafkaWriter() error {
	writerMutex.Lock()
	defer writerMutex.Unlock()

	if kafkaWriter == nil {
		kafkaWriter = kafka.NewWriter(kafka.WriterConfig{
			Brokers:     []string{kafkaBroker},
			Topic:       kafkaTopic,
			MaxAttempts: 5,
		})
	}
	return nil
}

func closeKafkaWriter() {
	writerMutex.Lock()
	defer writerMutex.Unlock()

	if kafkaWriter != nil {
		kafkaWriter.Close()
		kafkaWriter = nil
	}
}

// 캔들 데이터 패치
func fetchBTCCandleData(url string) ([]lib.CandleData, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var klines [][]interface{}
	err = json.Unmarshal(body, &klines)
	if err != nil {
		return nil, err
	}

	candles := make([]lib.CandleData, len(klines))
	for i, kline := range klines {
		candles[i] = lib.CandleData{
			OpenTime:  int64(kline[0].(float64)),
			Open:      kline[1].(string),
			High:      kline[2].(string),
			Low:       kline[3].(string),
			Close:     kline[4].(string),
			Volume:    kline[5].(string),
			CloseTime: int64(kline[6].(float64)),
		}
	}

	return candles, nil
}

func writeToKafka(ctx context.Context, candles []lib.CandleData) error {
	writerMutex.Lock()
	defer writerMutex.Unlock()

	if kafkaWriter == nil {
		return fmt.Errorf("kafka writer is not initialized")
	}

	jsonData, err := json.Marshal(candles)
	if err != nil {
		return err
	}

	return kafkaWriter.WriteMessages(ctx, kafka.Message{Value: jsonData})
}

func createWriter() *kafka.Writer {
	return kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:     []string{kafkaBroker},
			Topic:       kafkaTopic,
			MaxAttempts: 5,
		})
}

func utcToLocal(utcTime time.Time) time.Time {
	loc, err := time.LoadLocation("Asia/Seoul")
	if err != nil {
		log.Printf("Error loading location: %v\n", err)
		return utcTime
	}
	return utcTime.In(loc)
}

// / 다음 fetch 시간 구하는 함수
func nextIntervalStart(now time.Time, interval time.Duration) time.Time {
	return now.Truncate(interval).Add(interval)
}

// / 시간 반복에 따른 url에 넣을 String 반환 함수
func getIntervalString(interval time.Duration) string {
	switch interval {
	case 1 * time.Minute:
		return "1m"
	case 15 * time.Minute:
		return "15m"
	case 1 * time.Hour:
		return "1h"
	default:
		return "15m"
	}
}

// Service Discovery에 등록하는 함수
func registerService(writer *kafka.Writer) error {
	service := lib.Service{
		Name:    "abprice",
		Address: fmt.Sprintf("%s:%s", host, port),
	}

	jsonData, err := json.Marshal(service)
	if err != nil {
		return fmt.Errorf("error marshaling service data: %v", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(service.Name),
		Value: jsonData,
	})

	if err != nil {
		return fmt.Errorf("error sending registration message: %v", err)
	}

	log.Println("Service registration message sent successfully")
	return nil
}

// 서비스 등록 카프카 producer 생성
func createRegistrationWriter() *kafka.Writer {
	return kafka.NewWriter(
		kafka.WriterConfig{
			Brokers:     []string{kafkaBroker},
			Topic:       registrationTopic,
			MaxAttempts: 5,
		})
}

// 서비스 시작 함수
func startService(ctx context.Context) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		now := time.Now()
		nextFetch := nextIntervalStart(now, fetchInterval)
		sleepDuration := nextFetch.Sub(now)

		log.Printf("Waiting for %v until next fetch at %v\n", sleepDuration.Round(time.Second), nextFetch.Format("2006-01-02 15:04:05"))

		select {
		case <-time.After(sleepDuration):
			url := fmt.Sprintf("%s?symbol=BTCUSDT&interval=%s&limit=%d", binanceKlineAPI, getIntervalString(sleepDuration), candleLimit)

			candles, err := fetchBTCCandleData(url)
			if err != nil {
				log.Printf("Error fetching candle data: %v\n", err)
				continue
			}

			err = writeToKafka(ctx, candles)
			if err != nil {
				log.Printf("Error producing to Kafka: %v\n", err)
			} else {
				log.Printf("Successfully sent %d candle data to Kafka\n", len(candles))
				if len(candles) > 0 {
					firstCandle := candles[0]
					lastCandle := candles[len(candles)-1]
					firstTime := utcToLocal(time.Unix(firstCandle.OpenTime/1000, 0))
					lastTime := utcToLocal(time.Unix(lastCandle.CloseTime/1000, 0))
					log.Printf("Data range (Local Time): %v to %v\n",
						firstTime.Format("2006-01-02 15:04:05"),
						lastTime.Format("2006-01-02 15:04:05"))
				}
			}

		case <-signals:
			log.Println("Interrupt received, shutting down...")
			return

		case <-ctx.Done():
			log.Println("Context cancelled, shutting down...")
			return
		}
	}
}

// POST
// start service를 /start API로 받아서 실행하는 함수
func startHandler(c *gin.Context) {
	runningMutex.Lock()
	defer runningMutex.Unlock()

	if isRunning {
		c.JSON(http.StatusOK, gin.H{"message": "abprice is already running"})
		return
	}

	err := initKafkaWriter()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to initialize Kafka writer: %v", err)})
		return
	}

	isRunning = true
	go startService(serviceCtx)
	c.JSON(http.StatusOK, gin.H{"message": "abprice started successfully"})
}

// POST
// stop service를 /stop API로 받아서 실행하는 함수
// stop service 아직 미구현
func stopHandler(c *gin.Context) {
	runningMutex.Lock()
	defer runningMutex.Unlock()

	if !isRunning {
		c.JSON(http.StatusOK, gin.H{"message": "abprice is not running"})
		return
	}

	serviceCtxCancel() // 서비스 컨텍스트 취소
	closeKafkaWriter() // Kafka writer 닫기
	isRunning = false
	c.JSON(http.StatusOK, gin.H{"message": "abprice stopped successfully"})
}

// GET
// status를 /status API로 받아서 실행하는 함수
func statusHandler(c *gin.Context) {
	runningMutex.Lock()
	defer runningMutex.Unlock()

	status := "stopped"
	if isRunning {
		status = "running"
	}
	c.JSON(http.StatusOK, gin.H{"status": status})
}

func main() {
	writer := createWriter()
	defer writer.Close()

	registrationWriter := createRegistrationWriter()
	defer registrationWriter.Close()

	if err := registerService(registrationWriter); err != nil {
		log.Printf("Failed to register service: %v\n", err)
	}

	router := gin.Default()
	router.POST("/start", startHandler)
	router.POST("/stop", stopHandler)
	router.GET("/status", statusHandler)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	defer closeKafkaWriter()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server : %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	log.Println("Server exiting")
}
