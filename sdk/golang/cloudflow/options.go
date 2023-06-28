package cloudflow

type OPS map[string]interface{}

type CloudFlowOption interface {
	Get() OPS
}

type OptionInsCount struct {
	InsCount int
}

type OptionBatch struct {
	Batch int
}

func (self *OptionInsCount) Get() OPS {
	ops := OPS{
		"InsCount": self.InsCount,
	}
	return ops
}

func OpInsCount(count int) CloudFlowOption {
	return &OptionInsCount{
		InsCount: count,
	}
}

func ParsOptions(ops ...interface{}) ([]interface{}, map[string]interface{}) {
	exargs := []interface{}{}
	option := map[string]interface{}{}
	for _, v := range ops {
		switch v.(type) {
		case CloudFlowOption:
			for k, d := range v.(CloudFlowOption).Get() {
				option[k] = d
			}
		default:
			exargs = append(exargs, v)
		}
	}
	return exargs, option
}
