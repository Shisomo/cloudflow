package cloudflow

import "reflect"

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

func FilterByType(list []interface{}, tgt interface{}) ([]interface{}, []interface{}) {
	targets := []interface{}{}
	remains := []interface{}{}
	t_kinde := reflect.ValueOf(tgt).Type().Name()
	for _, v := range list {
		if t_kinde == reflect.ValueOf(v).Type().Name() {
			targets = append(targets, v)
		} else {
			remains = append(remains, v)
		}
	}
	return targets, remains
}

func ParsOptions(ops []interface{}) ([]interface{}, map[string]interface{}) {
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

// Define CloudFLow options here

func OpInsCount(count int) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"InsCount": count,
		}
	})
}

func OpInsRange(min int, max int) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"InsRange": []int{min, max},
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

func OpInsChannes(InChans [][]int) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"InChan": InChans,
		}
	})
}

func OpInType(itype string) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"InType": itype,
		}
	})
}

func OpOutType(otype string) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"OuType": otype,
		}
	})
}

func OpDispatchSize(size int) CloudFlowOption {
	return NewCFOption(func() OPS {
		return OPS{
			"DispSize": size,
		}
	})
}
