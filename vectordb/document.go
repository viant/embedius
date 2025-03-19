package vectordb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tmc/langchaingo/schema"
	"github.com/viant/bintly"
	"time"
)

type Document schema.Document

// EncodeBinary encodes data from binary stream
func (d *Document) EncodeBinary(stream *bintly.Writer) error {
	stream.String(d.PageContent)
	intKeys := make([]string, 0, len(d.Metadata))
	float32Keys := make([]string, 0, len(d.Metadata))
	float64Keys := make([]string, 0, len(d.Metadata))
	stringKeys := make([]string, 0, len(d.Metadata))
	timeKeys := make([]string, 0, len(d.Metadata))
	for k, v := range d.Metadata {
		switch v.(type) {
		case int:
			intKeys = append(intKeys, k)
		case float32:
			float32Keys = append(float32Keys, k)
		case float64:
			float64Keys = append(float64Keys, k)
		case string:
			stringKeys = append(stringKeys, k)
		case time.Time:
			timeKeys = append(timeKeys, k)
		default:
			return fmt.Errorf("unsupported EncodeBinary type %T", v)
		}
	}

	stream.Int16(int16(len(intKeys)))
	for _, k := range intKeys {
		stream.String(k)
		stream.Int(d.Metadata[k].(int))
	}

	stream.Int16(int16(len(float32Keys)))
	for _, k := range float32Keys {
		stream.String(k)
		stream.Float32(d.Metadata[k].(float32))
	}

	stream.Int16(int16(len(float64Keys)))
	for _, k := range float64Keys {
		stream.String(k)
		stream.Float64(d.Metadata[k].(float64))
	}

	stream.Int16(int16(len(stringKeys)))
	for _, k := range stringKeys {
		stream.String(k)
		stream.String(d.Metadata[k].(string))
	}

	stream.Int16(int16(len(timeKeys)))
	for _, k := range timeKeys {
		stream.String(k)
		stream.Time(d.Metadata[k].(time.Time))
	}
	return nil
}

// DecodeBinary decodes data to binary stream
func (d *Document) DecodeBinary(stream *bintly.Reader) error {
	stream.String(&d.PageContent)

	var size int16
	stream.Int16(&size)
	d.Metadata = make(map[string]interface{})
	for i := 0; i < int(size); i++ {
		var key string
		stream.String(&key)
		var value int
		stream.Int(&value)
		d.Metadata[key] = value
	}

	stream.Int16(&size)
	for i := 0; i < int(size); i++ {
		var key string
		stream.String(&key)
		var value float32
		stream.Float32(&value)
		d.Metadata[key] = value
	}

	stream.Int16(&size)
	for i := 0; i < int(size); i++ {
		var key string
		stream.String(&key)
		var value float64
		stream.Float64(&value)
		d.Metadata[key] = value
	}

	stream.Int16(&size)
	for i := 0; i < int(size); i++ {
		var key string
		stream.String(&key)
		var value string
		stream.String(&value)
		d.Metadata[key] = value
	}

	stream.Int16(&size)
	for i := 0; i < int(size); i++ {
		var key string
		stream.String(&key)
		var value time.Time
		stream.Time(&value)
		d.Metadata[key] = value
	}
	return nil
}

func (d *Document) Content() ([]string, error) {
	buffer := bytes.Buffer{}
	data, err := json.Marshal(d.Metadata)
	if err != nil {
		return nil, err
	}
	buffer.WriteString(string(data))
	buffer.WriteString(d.PageContent)
	item := string(buffer.Bytes())
	return []string{item}, nil
}
