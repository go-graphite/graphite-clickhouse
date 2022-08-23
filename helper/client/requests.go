package client

import protov3 "github.com/go-graphite/protocol/carbonapi_v3_pb"

type MultiGlobRequestV3 struct {
	protov3.MultiGlobRequest
}

func (r *MultiGlobRequestV3) Marshal() ([]byte, error) {
	return r.MultiGlobRequest.Marshal()
}

func (r *MultiGlobRequestV3) LogInfo() interface{} {
	return r.MultiGlobRequest
}
