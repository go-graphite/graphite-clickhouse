package data

// CHResponse contains the parsed Data and From/Until timestamps
type CHResponse struct {
	Data  *Data
	From  int64
	Until int64
}

// CHResponses is a slice of CHResponse
type CHResponses []CHResponse

// EmptyResponse returns an CHResponses with one element containing emptyData for the following encoding
func EmptyResponse() CHResponses { return CHResponses{{emptyData, 0, 0}} }
