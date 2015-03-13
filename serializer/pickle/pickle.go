package pickle

import (
	"bytes"

	"github.com/hydrogen18/stalecucumber"
)

type PickleSerializer struct {
}

func (p *PickleSerializer) Serialize(o interface{}) (output []byte, err error) {
	buf := new(bytes.Buffer)
	//BUG: this doesn't work for some reason. It always serialize into 0 bytes
	_, err = stalecucumber.NewPickler(buf).Pickle(o)
	output = buf.Bytes()
	return
}

func (p *PickleSerializer) Deserialize(b []byte, o interface{}) (err error) {
	unpacker := stalecucumber.UnpackInto(o)
	//BUG: this doens't work as pickle cannot deserialize kwargs!!
	unpacker.AllowMismatchedFields = true
	unpacker.AllowMissingFields = true
	err = unpacker.From(stalecucumber.Unpickle(bytes.NewReader(b)))
	//log.Debugf("Pickled: [%s] [%s]", task.ID, task.Task)
	return
}
