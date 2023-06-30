package cloudflow

type OPS map[string]interface{}
type OptionFunc func() OPS

type CloudFlowOption interface {
	Get() OPS
}

type CFOption struct {
	option_fc OptionFunc
}

func (self *CFOption) Get() OPS {
	return self.option_fc()
}

func NewCFOption(fc OptionFunc) *CFOption {
	return &CFOption{
		option_fc: fc,
	}
}

func OpInsCount(count int) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"InsCount": count,
		}
	})
}

func OpPerfLogInter(interval int) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"InsCount": interval,
		}
	})
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
