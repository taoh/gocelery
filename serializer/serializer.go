package serializer

import (
	"errors"

	"github.com/taoh/gocelery/serializer/json"
	"github.com/taoh/gocelery/serializer/pickle"
)

// Serializer converst he format
type Serializer interface {
	Serialize(interface{}) ([]byte, error)
	Deserialize([]byte, interface{}) error
}

var serializerRegistry = make(map[string]Serializer)

var (
	ErrSerializerNotFound = errors.New("Serializer not found")
)

func RegisterSerializer(name string, s Serializer) {
	serializerRegistry[name] = s
}

func NewSerializer(name string) (Serializer, error) {
	if serializer, ok := serializerRegistry[name]; ok { // check if scheme is registered
		return serializer, nil
	}
	return nil, ErrSerializerNotFound
}

func init() {
	RegisterSerializer("application/x-python-serialize", &pickle.PickleSerializer{})
	RegisterSerializer("application/json", &json.JsonSerializer{})
}
