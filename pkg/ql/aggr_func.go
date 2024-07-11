package ql

import (
	"fmt"

	"github.com/c4pt0r/kvql"
)

// func RegisterAggrFuncs() {
// 	kvql.AddAggrFunction(distinct)
// }

var Distinct = &kvql.AggrFunc{
	Name:       "distinct",
	NumArgs:    1,
	VarArgs:    false,
	ReturnType: kvql.TLIST,
	Body:       newDistinctFunc,
}

type aggrDistinctFunc struct {
	args     []kvql.Expression
	distinct map[string]bool
}

func (d *aggrDistinctFunc) Update(kv kvql.KVPair, args []kvql.Expression, ctx *kvql.ExecuteCtx) error {
	arg, err := args[0].Execute(kv, ctx)
	if err != nil {
		return err
	}
	k := toString(arg)
	fmt.Println("Inserting value:", k)
	d.distinct[k] = true
	return nil
}

func (d *aggrDistinctFunc) Complete() (any, error) {
	distinctArr := make([]string, len(d.distinct))
	var i int
	for k := range d.distinct {
		distinctArr[i] = k
		i++
	}
	return distinctArr, nil
}

func (d *aggrDistinctFunc) Clone() kvql.AggrFunction {
	return &aggrDistinctFunc{
		args:     d.args,
		distinct: d.distinct,
	}
}

func newDistinctFunc(args []kvql.Expression) (kvql.AggrFunction, error) {
	return &aggrDistinctFunc{
		args:     args,
		distinct: make(map[string]bool),
	}, nil
}

func toString(value any) string {
	switch val := value.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	default:
		if val == nil {
			return "<nil>"
		}
		return ""
	}
}
