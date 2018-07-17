package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

// Config represents common configs
type Config struct {
	Key      string   `mapstructure:"key"`
	Secret   string   `mapstructure:"secret"`
	CertFile string   `mapstructure:"cert_file"`
	Servers  []string `mapstructure:"servers"`
	Topics   []string `mapstructure:"topics"`
	Host     string   `mapstructure:"host"`
	Port     int      `mapstructure:"port"`
}

// ConfigName is filename of config file without extension
const ConfigName = "khub"

// Event represents event data
type Event struct {
	Subject string `json:"subject"`
	Actor   string `json:"actor"`
	Action  string `json:"action"`
	Target  string `json:"target"`
}

func makeConfig() (*Config, error) {
	configPath := os.Getenv("CONFIG_PATH")
	if len(configPath) == 0 {
		configPath = "config"
	}
	var v = viper.New()
	v.AddConfigPath(configPath)
	v.SetConfigName(ConfigName)
	if err := v.ReadInConfig(); err != nil {
		return &Config{}, err
	}

	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return &Config{}, err
	}

	return &config, nil
}

func makeProducer(cfg *Config) (sarama.SyncProducer, error) {
	fmt.Print("create Kafka producer, it may take a few seconds\n")

	saramaConfig := sarama.NewConfig()
	saramaConfig.Net.SASL.Enable = true
	saramaConfig.Net.SASL.User = cfg.Key
	saramaConfig.Net.SASL.Password = cfg.Secret
	saramaConfig.Net.TLS.Enable = true
	saramaConfig.Producer.Return.Successes = true

	var producer sarama.SyncProducer
	var err error

	certBytes, err := ioutil.ReadFile(cfg.CertFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read cert file: %s", cfg.CertFile)
	}
	clientCertPool := x509.NewCertPool()
	ok := clientCertPool.AppendCertsFromPEM(certBytes)
	if !ok {
		return nil, fmt.Errorf("failed to parse cert file")
	}
	saramaConfig.Net.TLS.Config = &tls.Config{
		RootCAs:            clientCertPool,
		InsecureSkipVerify: true,
	}

	if err = saramaConfig.Validate(); err != nil {
		return nil, fmt.Errorf("Kafka producer config invalidate: %v", err)
	}

	producer, err = sarama.NewSyncProducer(cfg.Servers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("Kafak producer create fail: %v", err)
	}

	return producer, nil
}

func kafkaProduce(producer sarama.SyncProducer, topic string, key string, content string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(content),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		msg := fmt.Sprintf("Send Error topic: %v. key: %v. content: %v", topic, key, content)
		fmt.Println(msg)
		return err
	}

	fmt.Printf("Send OK topic:%s key:%s value:%s, partition:%d, offset:%d,\n",
		topic, key, content, partition, offset)

	return nil
}

func testAVRO() {
	const schema = `{
		"namespace": "moremom.service.event",
		"type": "record",
		"name": "Event",
		"doc": "Avro Schema for our Event",
		"fields": [
			{"name": "subject", "type": "string"},
			{"name": "action", "type": "string"},
			{"name": "actor",  "type": "string"},
			{"name": "target", "type": "string"}
		]
	}`
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		panic(err)
	}

	textual := []byte(`{"subject":"carer_application", "actor":"12345678", "action":"verify", "target":"21415710"}`)
	native, _, err := codec.NativeFromTextual(textual)
	if err != nil {
		panic(err)
	}
	println(native)

	// Convert native Go form to binary Avro data
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}

	// Convert binary Avro data back to native Go form
	native, _, err = codec.NativeFromBinary(binary)
	if err != nil {
		fmt.Println(err)
	}

	// Convert native Go form to textual Avro data
	textual, err = codec.TextualFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}
}

type publishEventRequest struct {
	Event Event `json:"event"`
}

type handlerFunc func(w http.ResponseWriter, r *http.Request)

func decodeRequest(r *http.Request) (req publishEventRequest, err error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return publishEventRequest{}, errors.Wrap(err, "ioutil.ReadAll error")
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return publishEventRequest{}, errors.Wrap(err, "json.Unmarshal error")
	}
	return
}

func writeResponse(w http.ResponseWriter, r *http.Request, status int, data interface{}) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return err
	}
	w.WriteHeader(status)
	if _, err := io.Copy(w, &buf); err != nil {
		return err
	}
	return nil
}

func makePublishEventHandler(producerService *ProducerService) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST Required", http.StatusMethodNotAllowed)
			return
		}

		req, err := decodeRequest(r)
		if err != nil {
			http.Error(w, "Invalid Parameters", http.StatusBadRequest)
			return
		}

		result := producerService.produceEvent(req.Event)

		writeResponse(w, r, http.StatusOK, result)
	}
}

// ProducerService represents stuff to produce Kafka message
type ProducerService struct {
	Config        *Config
	KafkaProducer sarama.SyncProducer
	SchemaCodec   *goavro.Codec
}

// EventSchema is Avro schema definition for our event
const EventSchema = `{
	"namespace": "moremom.service.event",
	"type": "record",
	"name": "Event",
	"doc": "Avro schema for our event",
	"fields": [
		{"name": "subject", "type": "string"},
		{"name": "actor",  "type": "string"},
		{"name": "action", "type": "string"},
		{"name": "target", "type": "string"}
	]
}`

func (ps *ProducerService) produceEvent(event Event) error {
	// TODO: event.Subject -> topic
	topic := ps.Config.Topics[0]
	key := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)

	mapEvent := map[string]string{
		"subject": event.Subject,
		"actor":   event.Actor,
		"action":  event.Action,
		"target":  event.Target,
	}

	data, err := ps.SchemaCodec.BinaryFromNative(nil, mapEvent)
	if err != nil {
		return errors.Wrap(err, "BinaryFromNative error")
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(data),
	}

	partition, offset, err := ps.KafkaProducer.SendMessage(msg)
	if err != nil {
		msg := fmt.Sprintf("Send Error topic: %v. key: %v. content: %v", topic, key, data)
		fmt.Println(msg)
		return err
	}

	fmt.Printf("Send OK topic:%s key:%s value:%s, partition:%d, offset:%d,\n",
		topic, key, data, partition, offset)

	if err != nil {
		panic(err)
	}

	return nil
}

func newProducerService(config *Config) *ProducerService {
	producer, err := makeProducer(config)
	if err != nil {
		panic(fmt.Errorf("fatal error makeProducer: %s", err))
	}

	codec, err := goavro.NewCodec(EventSchema)
	if err != nil {
		panic(fmt.Errorf("fatal error goavro.NewCodec: %s", err))
	}

	return &ProducerService{
		Config:        config,
		KafkaProducer: producer,
		SchemaCodec:   codec,
	}
}

func main() {
	config, err := makeConfig()
	if err != nil {
		panic(fmt.Errorf("fatal error makeConfig: %s", err))
	}

	producerService := newProducerService(config)

	http.HandleFunc("/event/publish", makePublishEventHandler(producerService))

	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	log.Println("http listening at: ", addr)

	err = http.ListenAndServe(addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe error: ", err)
	}
}
