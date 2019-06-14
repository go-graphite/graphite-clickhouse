package kafka

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

func init() {
	zapwriter.RegisterScheme("kafka", New)
}

type KafkaOutput struct {
	sync.RWMutex
	addrs         []string
	topic         string
	sync          bool
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
	config        *sarama.Config
	errorLogger   string // fallback logger
	exit          chan interface{}
	exitOnce      sync.Once
	exitWg        sync.WaitGroup
}

func New(path string) (zapwriter.Output, error) {
	u, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	k := sarama.NewConfig()

	params := zapwriter.DSN(u.Query())
	sync, _ := params.Bool("sync", false)

	if u.User.Username() != "" {
		k.Net.SASL.Enable = true
		k.Net.SASL.User = u.User.Username()
		k.Net.SASL.Password, _ = u.User.Password()
	}

	if err := zapwriter.AnyError(
		params.SetDuration(&k.Net.DialTimeout, "net.timeout"),
		params.SetDuration(&k.Net.ReadTimeout, "net.timeout"),
		params.SetDuration(&k.Net.WriteTimeout, "net.timeout"),
		params.SetDuration(&k.Net.KeepAlive, "net.keep_alive"),
		params.SetInt(&k.Metadata.Retry.Max, "metadata.retry.max"),
		params.SetDuration(&k.Metadata.Retry.Backoff, "metadata.retry.backoff"),
		params.SetDuration(&k.Metadata.RefreshFrequency, "metadata.refresh_frequency"),
		params.SetInt(&k.Producer.MaxMessageBytes, "max_message_bytes"),
		params.SetDuration(&k.Producer.Timeout, "timeout"),
		params.SetInt(&k.Producer.Flush.Bytes, "flush.bytes"),
		params.SetInt(&k.Producer.Flush.Messages, "flush.messages"),
		params.SetDuration(&k.Producer.Flush.Frequency, "flush.frequency"),
		params.SetInt(&k.Producer.Flush.MaxMessages, "flush.max_messages"),
		params.SetInt(&k.Producer.Retry.Max, "retry.max"),
		params.SetDuration(&k.Producer.Retry.Backoff, "retry.backoff"),
		params.SetString(&k.ClientID, "client_id"),
		params.SetInt(&k.ChannelBufferSize, "channel_buffer_size"),
	); err != nil {
		return nil, err
	}

	var ok bool

	if k.Producer.RequiredAcks, ok = map[string]sarama.RequiredAcks{
		"":      k.Producer.RequiredAcks,
		"no":    sarama.NoResponse,
		"local": sarama.WaitForLocal,
		"all":   sarama.WaitForAll,
	}[params.Get("required_acks")]; !ok {
		return nil, fmt.Errorf("unknown value of required_acks: %#v", params.Get("required_acks"))
	}

	if k.Producer.Compression, ok = map[string]sarama.CompressionCodec{
		"":       k.Producer.Compression,
		"no":     sarama.CompressionNone,
		"none":   sarama.CompressionNone,
		"gz":     sarama.CompressionGZIP,
		"gzip":   sarama.CompressionGZIP,
		"snappy": sarama.CompressionSnappy,
		"lz4":    sarama.CompressionLZ4,
	}[params.Get("compression")]; !ok {
		return nil, fmt.Errorf("unknown value of compression: %#v", params.Get("compression"))
	}

	version, err := params.String("version", "")
	if err != nil {
		return nil, err
	}
	if version != "" {
		kafkaVersion, err := sarama.ParseKafkaVersion(version)
		if err != nil {
			return nil, err
		}
		k.Version = kafkaVersion
	}

	k.Producer.Return.Successes = sync
	k.Producer.Return.Errors = sync

	if err := k.Validate(); err != nil {
		return nil, err
	}

	topic, err := params.StringRequired("topic")
	if err != nil {
		return nil, err
	}

	errorLogger, err := params.String("error_logger", "")
	if err != nil {
		return nil, err
	}

	r := &KafkaOutput{
		config:      k,
		addrs:       strings.Split(u.Host, ","),
		sync:        sync,
		topic:       topic,
		errorLogger: errorLogger,
	}

	return r, nil
}

func (r *KafkaOutput) getSyncProducer() (sarama.SyncProducer, error) {
	r.RLock()
	p := r.syncProducer
	r.RUnlock()

	var err error

	if p == nil {
		r.Lock()
		p = r.syncProducer
		if p != nil {
			r.Unlock()
			return p, nil
		}

		p, err = sarama.NewSyncProducer(r.addrs, r.config)
		if err != nil {
			r.Unlock()
			return nil, err
		}

		go func(p sarama.SyncProducer) {
			<-r.exit
			p.Close()
		}(p)
		r.syncProducer = p
		r.Unlock()

	}

	return p, nil
}

func (r *KafkaOutput) getAsyncProducer() (sarama.AsyncProducer, error) {
	r.RLock()
	p := r.asyncProducer
	r.RUnlock()

	var err error

	if p == nil {
		r.Lock()
		p = r.asyncProducer
		if p != nil {
			r.Unlock()
			return p, nil
		}

		p, err = sarama.NewAsyncProducer(r.addrs, r.config)
		if err != nil {
			r.Unlock()
			return nil, err
		}

		go func(p sarama.AsyncProducer) {
			<-r.exit
			p.Close()
		}(p)
		r.asyncProducer = p
		r.Unlock()

	}

	return p, nil
}

func (r *KafkaOutput) writeSync(p []byte) (int, error) {
	for {
		select {
		case <-r.exit:
			return 0, fmt.Errorf("aborted")
		default:
			// pass
		}

		producer, err := r.getSyncProducer()
		if err != nil {
			zapwriter.Logger(r.errorLogger).Error("sync send to kafka failed, retrying", zap.String("message", string(p)), zap.Error(err))
			continue
		}

		_, _, err = producer.SendMessage(&sarama.ProducerMessage{
			Topic:     r.topic,
			Key:       sarama.StringEncoder(""),
			Value:     sarama.ByteEncoder(p),
			Timestamp: time.Now(),
		})

		if err == nil {
			return len(p), nil
		}

		if r.errorLogger != "" {
			zapwriter.Logger(r.errorLogger).Error("sync send to kafka failed, retrying", zap.String("message", string(p)), zap.Error(err))
		}
	}
}

func (r *KafkaOutput) writeAsync(p []byte) (int, error) {
	producer, err := r.getAsyncProducer()
	if err != nil {
		return 0, err
	}

	msg := &sarama.ProducerMessage{
		Topic:     r.topic,
		Key:       sarama.StringEncoder(""),
		Value:     sarama.ByteEncoder(p),
		Timestamp: time.Now(),
	}

	select {
	case producer.Input() <- msg:
		return len(p), nil
	case <-r.exit:
		return 0, fmt.Errorf("aborted")
	}
}

func (r *KafkaOutput) Write(p []byte) (n int, err error) {
	if r.sync {
		n, err = r.writeSync(p)
	} else {
		n, err = r.writeAsync(p)
	}

	if err != nil && r.errorLogger != "" {
		zapwriter.Logger(r.errorLogger).Error("send to kafka failed", zap.String("message", string(p)), zap.Error(err))
	}
	return
}

func (r *KafkaOutput) Sync() (err error) {
	return
}

func (r *KafkaOutput) Close() (err error) {
	r.exitOnce.Do(func() {
		close(r.exit)
	})
	r.exitWg.Wait()
	return
}
