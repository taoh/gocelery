package json

import "encoding/json"

type JsonSerializer struct {
}

func (p *JsonSerializer) Serialize(o interface{}) (bytes []byte, err error) {
	bytes, err = json.Marshal(o)
	return
}

func (p *JsonSerializer) Deserialize(bytes []byte, o interface{}) (err error) {
	return json.Unmarshal(bytes, &o)
}
