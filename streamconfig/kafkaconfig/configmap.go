package kafkaconfig

import (
	"reflect"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const tagIdentifier = "kafka"

type mapper interface {
	ConfigValue() kafka.ConfigValue
}

type tag string

func (t tag) Has(s string) bool {
	ts := strings.Split(string(t), ",")
	for i := range ts {
		if ts[i] == s {
			return true
		}
	}

	return false
}

func (t tag) Name() string {
	return strings.Split(string(t), ",")[0]
}

func configMap(ifaces ...interface{}) *kafka.ConfigMap {
	cfg := &kafka.ConfigMap{}

	for i := range ifaces {
		parse(cfg, ifaces[i], []string{})
	}

	return cfg
}

func setKey(cfg *kafka.ConfigMap, ks []string, iface interface{}) {
	_ = cfg.SetKey(strings.Join(ks, "."), iface) // nolint: gas
}

func parse(cfg *kafka.ConfigMap, iface interface{}, ks []string) {
	// If the interface has a `ConfigValue()` method, we call that, and set the
	// resulting configuration value.
	if m, ok := iface.(mapper); ok {
		setKey(cfg, ks, m.ConfigValue())

		return
	}

	v := reflect.ValueOf(iface)

	// Depending on the type of object we're dealing with, we either convert the
	// object to the right configuration value, or go into the object and loop its
	// fields.
	switch v.Kind() {
	case reflect.Ptr:
		v = v.Elem()
		fallthrough
	case reflect.Struct:
		parseStruct(cfg, v.Interface(), ks)
	case reflect.Slice, reflect.Array:
		parseSlice(cfg, v.Interface(), ks)
	case reflect.Int64:
		if strings.HasSuffix(strings.Join(ks, "."), ".ms") {
			parseDuration(cfg, v.Interface(), ks)
			return
		}

		fallthrough
	default:
		setKey(cfg, ks, v.Interface())
	}
}

func parseStruct(cfg *kafka.ConfigMap, iface interface{}, ks []string) {
	v := reflect.ValueOf(iface)
	t := v.Type()

	// We're dealing with a struct here, so let's loop over the fields, and see
	// what to do with them.
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)

		// We skip unexported fields, since we can't access their value.
		if f.PkgPath != "" {
			continue
		}

		// Get the tag properties for this field
		tags := tag(f.Tag.Get(tagIdentifier))

		// skip any fields that have their tag set to `-`
		if tags == "-" {
			continue
		}

		// Get the value of the field
		fv := v.FieldByName(f.Name)

		// skip any fields that have a zero value (nil, empty strings, 0, etc) and
		// have the `omitempty` tag set.
		zero := reflect.Zero(fv.Type()).Interface()
		if tags.Has("omitempty") && reflect.DeepEqual(fv.Interface(), zero) {
			continue
		}

		// set the config name to either the tag name, or the lower-cased name of
		// the field.
		k := tags.Name()
		if k == "" {
			k = strings.ToLower(f.Name)
		}

		parse(cfg, fv.Interface(), append(ks, k))
	}
}

func parseSlice(cfg *kafka.ConfigMap, iface interface{}, ks []string) {
	values, ok := iface.([]string)
	if !ok || len(values) == 0 {
		return
	}

	setKey(cfg, ks, strings.Join(values, ","))
}

func parseDuration(cfg *kafka.ConfigMap, iface interface{}, ks []string) {
	v := int(iface.(time.Duration) / time.Millisecond)

	setKey(cfg, ks, v)
}
