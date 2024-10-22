package activity

import (
	"fmt"
	"testing"
)

func TestFoo(t *testing.T) {
	m := Module{}
	typ := m.Components()[0].ReflectType()
	fmt.Println(typ.PkgPath(), typ.Name())
}
