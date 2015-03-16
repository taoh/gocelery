package serializer

import (
	"fmt"

	"github.com/taoh/gocelery/serializer/json"
	"github.com/taoh/gocelery/serializer/pickle"
)

// Serializer converst he format
type Serializer interface {
	Serialize(interface{}) ([]byte, error)
	Deserialize([]byte, interface{}) error
}

var serializerRegistry = make(map[string]Serializer)

// RegisterSerializer adds a new serializer
func RegisterSerializer(contentType string, s Serializer) {
	serializerRegistry[contentType] = s
}

// NewSerializer returns the serializer from the given content type
func NewSerializer(contentType string) (Serializer, error) {
	if serializer, ok := serializerRegistry[contentType]; ok { // check if scheme is registered
		return serializer, nil
	}
	err := fmt.Errorf("Serializer not found for content type [%s]", contentType)
	return nil, err
}

func init() {
	RegisterSerializer("application/x-python-serialize", &pickle.PickleSerializer{})
	RegisterSerializer("application/json", &json.JsonSerializer{})
}
