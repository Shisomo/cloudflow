package main

import (
	"fmt"
	"reflect"
)

func copyAs(data interface{}, tgt reflect.Type) interface{} {
	fmt.Print("*")
	ref_data := reflect.ValueOf(data)
	kind := tgt.Kind()

	switch kind {
	case reflect.Slice:
		fmt.Print("1")
		length := ref_data.Len()
		ret := reflect.MakeSlice(tgt, length, length)
		fmt.Println("ret.val")
		for i := 0; i < length; i++ {
			value := copyAs(ref_data.Index(i), ret.Index(0).Type())
			ret.Index(i).Set(reflect.ValueOf(value))
		}
		return ret.Elem()
	case reflect.Map:
		fmt.Print("2")
		//data := data.(map[string]interface{})
		ret := reflect.MakeMap(tgt)
		//for k, v := range data {
		//ret.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(copyAs(v, data)))
		//}
		return ret.Elem()
	case reflect.Bool:
		fmt.Print("3")
		return bool(data.(bool))
	//case reflect.Int:
	//	fmt.Print("4")
	//	return int(data.(float64))
	default:
		fmt.Print("5")
		return data
	}
}

func a(x int, y string, z []int, m []int) {

}

func main() {
	ref_fc := reflect.ValueOf(a)
	fc_type := ref_fc.Type()
	a := []int{1, 2}
	fmt.Println(copyAs(a, fc_type.In(3)))
}
