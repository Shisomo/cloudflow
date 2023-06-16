package test

import (
	cf "cloudflow/sdk/golang/cloudflow/comm"
	"reflect"
	"testing"
)

func a(x int, y string, z []int, m map[int][]int) {

}

func Test_TypeConvert(t *testing.T) {
	ref_fc := reflect.ValueOf(a)
	fc_type := ref_fc.Type()
	a := map[interface{}][]float64{
		1: {1.0, 2.0},
		2: {1.0, 2.0},
	}
	ttype := fc_type.In(3)
	v := cf.JAsType(a, ttype)
	vtype := reflect.TypeOf(v)

	t.Log("value:=", v, "type:=", vtype, "target:=", ttype)
	cf.Assert(cf.Astr(vtype) == cf.Astr(ttype), "type convert fail: %s != %s", cf.Astr(vtype), cf.Astr(ttype))
}
