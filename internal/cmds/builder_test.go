package cmds

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var (
	keysDynamic1000 = func() []string {
		ks := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			ks[i] = "key_" + strconv.Itoa(i)
		}
		return ks
	}()
	keysTag1000 = func() []string {
		ks := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			ks[i] = "{user:" + strconv.Itoa(i) + "}:profile"
		}
		return ks
	}()
	keysSameSlot50 = func() []string {
		ks := make([]string, 50)
		for i := 0; i < 50; i++ {
			ks[i] = "{user:1}:key_" + strconv.Itoa(i)
		}
		return ks
	}()
	payload64B  = strings.Repeat("a", 64)
	payload1KB  = strings.Repeat("b", 1024)
	payload64KB = strings.Repeat("c", 64*1024)
)

// Benchmark_Builder_SingleCommand_DynamicKeys measures single command construction with dynamic keys.
func Benchmark_Builder_SingleCommand_DynamicKeys(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keysDynamic1000[i%1000]
		cmd := builder.Set().Key(k).Value(payload1KB).Build()
		PutCompleted(cmd)
	}
}

// Benchmark_Builder_MultiKey_Scaling measures multi-key MGet building across cardinality gradient.
func Benchmark_Builder_MultiKey_Scaling(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := builder.Mget().Key(keysSameSlot50...).Build()
		PutCompleted(cmd)
	}
}

// Benchmark_Builder_ComplexArgs_HSet_ZAdd measures complex argument building with cardinality gradient.
func Benchmark_Builder_ComplexArgs_HSet_ZAdd(b *testing.B) {
	builder := NewBuilder(InitSlot)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		z := builder.Zadd().Key(keysDynamic1000[i%1000]).ScoreMember()
		for j := 0; j < 10; j++ {
			z = z.ScoreMember(float64(j), keysDynamic1000[j])
		}
		cmd := z.Build()
		PutCompleted(cmd)
	}
}

// Benchmark_MemoryPool_PutCompleted_Parallel verifies zero-allocation pool reclamation under concurrency.
func Benchmark_MemoryPool_PutCompleted_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		builder := NewBuilder(InitSlot)
		i := 0
		for pb.Next() {
			k := keysDynamic1000[i%1000]
			cmd := builder.Get().Key(k).Build()
			PutCompleted(cmd)
			i++
		}
	})
}

// Benchmark_Cluster_CRC16_Routing measures CRC16 slot calculation for plain vs {hash_tag} keys.
func Benchmark_Cluster_CRC16_Routing(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = slot(keysTag1000[i%1000])
		_ = slot(keysDynamic1000[i%1000])
	}
}

// Benchmark_ZAdd is kept for backwards compatibility.
func Benchmark_ZAdd(b *testing.B) {
	Benchmark_Builder_ComplexArgs_HSet_ZAdd(b)
}

// BenchmarkCommandBuilder_Allocation is kept for backwards compatibility.
func BenchmarkCommandBuilder_Allocation(b *testing.B) {
	Benchmark_Builder_SingleCommand_DynamicKeys(b)
}

func TestPutCompleted(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	PutCompleted(Completed{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("Put doesn't clean the CommandSlice")
	}
}

func TestPutCompletedForce(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	cs1.r = 1 // pin
	PutCompletedForce(Completed{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("PutCompletedForce doesn't clean the CommandSlice")
	}
}

func TestPutCacheableForce(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	cs1.r = 1 // pin
	PutCacheableForce(Cacheable{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("PutCacheableForce doesn't clean the CommandSlice")
	}
}

func TestPutCacheable(t *testing.T) {
retry:
	cs1 := get()
	cs1.s = append(cs1.s, "1", "1", "1", "1", "1")
	PutCacheable(Cacheable{cs: cs1})
	cs2 := get()
	if cs1 != cs2 {
		goto retry
	}
	if len(cs2.s) != 0 {
		t.Fatalf("Put doesn't clean the CommandSlice")
	}
}

func TestArbitraryIsZero(t *testing.T) {
	builder := NewBuilder(NoSlot)
	if cmd := builder.Arbitrary("any", "cmd"); cmd.IsZero() {
		t.Fatalf("arbitrary failed")
	}
	var cmd Arbitrary
	if !cmd.IsZero() {
		t.Fatalf("arbitrary failed")
	}
}

func TestArbitrary(t *testing.T) {
	builder := NewBuilder(NoSlot)
	cmd := builder.Arbitrary("any", "cmd").Keys("k1", "k2").Args("a1", "a2")
	if c := cmd.Build(); !reflect.DeepEqual(c.Commands(), []string{"any", "cmd", "k1", "k2", "a1", "a2"}) {
		t.Fatalf("arbitrary failed")
	}
	if c := builder.Arbitrary("any").Blocking(); !c.IsBlock() {
		t.Fatalf("arbitrary failed")
	}
	if c := builder.Arbitrary("any").ReadOnly(); !c.IsReadOnly() {
		t.Fatalf("arbitrary failed")
	}

	builder2 := NewBuilder(InitSlot)

	defer func() {
		if e := recover(); e != multiKeySlotErr {
			t.Errorf("arbitrary not check slots")
		}
	}()

	builder2.Arbitrary().Keys("k1", "k2")
}

func TestEmptyArbitrary(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitraryNoCommand {
			t.Errorf("arbitrary not check empty")
		}
	}()
	builder.Arbitrary().Build()
}

func TestEmptySubscribe(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitrarySubscribe {
			t.Errorf("arbitrary not check subscribe command")
		}
	}()
	builder.Arbitrary("SUBSCRIBE").Build()
}

func TestEmptyArbitraryMultiGet(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitraryNoCommand {
			t.Errorf("arbitrary not check empty")
		}
	}()
	builder.Arbitrary().MultiGet()
}

func TestArbitraryMultiGet(t *testing.T) {
	builder := NewBuilder(NoSlot)
	cacheable := Cacheable(builder.Arbitrary("MGET").Args("KKK").MultiGet())
	if !cacheable.IsMGet() {
		t.Fatalf("arbitrary failed")
	}
}

func TestArbitraryMultiGetPanic(t *testing.T) {
	builder := NewBuilder(NoSlot)
	defer func() {
		if e := recover(); e != arbitraryMultiGet {
			t.Errorf("arbitrary not check MGET command")
		}
	}()
	builder.Arbitrary("SUBSCRIBE").MultiGet()
}

func TestBuiltTwice(t *testing.T) {
	src := NewBuilder(NoSlot).Get()
	cmd1 := src.Key("a")
	cmd2 := src.Key("b")
	cmd1.Build()
	defer func() {
		if e := recover(); e != ErrBuiltTwice {
			t.Errorf("arbitrary not check MGET command")
		}
	}()
	cmd2.Build()
}

func TestVerify(t *testing.T) {
	src := NewBuilder(NoSlot).Get()
	cmd1 := src.Key("a").Build()
	cmd1.cs.Verify()
	src.Key("b")
	defer func() {
		if e := recover(); e != ErrUnfinished {
			t.Errorf("arbitrary not check MGET command")
		}
	}()
	cmd1.cs.Verify()
}
