package domain

import (
	"sync"
	"testing"
	"time"
)

func Test_IDSet_TakeIDs(t *testing.T) {
	type fields struct {
		ranges   []IDRange
		category string
		readOnly bool
	}
	type args struct {
		setSize uint64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantOut   *IDSet
		wantState uint64
		wantErr   bool
	}{
		{
			fields: fields{
				ranges: []IDRange{
					NewIDRange(1, 1000, false),
					NewIDRange(1001, 2000, false),
					NewIDRange(2001, 3000, false),
				},
				category: OperationIdCategory,
			},
			wantOut: NewSet(idSet{
				Ranges: []IDRange{
					NewIDRange(1, 1000, false),
					NewIDRange(1001, 1450, false),
				},
				Category: OperationIdCategory,
			}),
			wantState: 1550,
			args: args{
				setSize: 1450,
			},
		},
	}
	for i := range tests {
		tt := &tests[i]
		t.Run(tt.name, func(t *testing.T) {
			s := NewSet(idSet{
				Ranges:   tt.fields.ranges,
				Category: tt.fields.category,
				readOnly: tt.fields.readOnly,
			})
			got, err := s.TakeIDs(tt.args.setSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.takeIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				return
			}
			t.Logf(got.String())
			if got.GetSize() != tt.wantOut.GetSize() {
				t.Errorf("OUT: idSet.takeIDs().getSizeCh() = %d, wantOut.getSizeCh() %d",
					got.GetSize(), tt.wantOut.GetSize())
			}
			if s.GetSize() != tt.wantState {
				t.Errorf("STATE: idSet.getSizeCh() = %d, wantState %d",
					got.GetSize(), tt.wantState)
			}
		})
	}
}

func Test_IDSet_TakeIDsConc(t *testing.T) {
	type fields struct {
		ranges   []IDRange
		category string
		readOnly bool
	}
	type args struct {
		setSize uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			fields: fields{
				ranges: []IDRange{
					NewIDRange(1, 100, false),
					NewIDRange(201, 300, false),
					NewIDRange(401, 500, false),
					NewIDRange(601, 700, false),
					NewIDRange(701, 800, false),
					NewIDRange(901, 1000, false),
				},
				category: OperationIdCategory,
			},
			args: args{
				setSize: 2,
			},
		},
	}
	wg := sync.WaitGroup{}
	tt := &tests[0]
	s := NewSet(idSet{
		Ranges:   tt.fields.ranges,
		Category: tt.fields.category,
		readOnly: tt.fields.readOnly,
	})
	for j := 0; j < 300; j++ {

		wg.Add(1)
		go func(st *IDSet) {
			defer wg.Done()
			_, err := st.TakeIDs(tt.args.setSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.takeIDs() error = %v, wantErr %v", err, tt.wantErr)
			}
		}(s)
	}
	wg.Wait()
	if s.GetSize() != 0 {
		t.Errorf("idSet.takeIDs() error = %v, wantSize %v", s.GetSize(), 0)
	}
}

func Test_IDSet_TakeID(t *testing.T) {
	wg := sync.WaitGroup{}

	set := NewIDSet([]IDRange{
		NewIDRange(1, 1000000000, false),
	}, OperationIdCategory, false)

	const noGoroutines = 100
	const setSize = 100
	const take = setSize / 2
	const wantSize = 1000000000 - noGoroutines*take

	for i := 0; i < noGoroutines; i++ {
		wg.Add(1)
		go func(in int, s *IDSet) {
			defer wg.Done()
			out, err := s.TakeIDs(setSize)
			if err != nil {
				t.Errorf("ERROR: IDSet.TakeIDs() error = %v", err)
				return
			}
			outStr := out.String()
			t.Logf("g-%d: take out: \t\t%s", in, outStr)

			for j := 0; j < take; j++ {
				_, err := out.TakeID()
				if err != nil {
					t.Errorf("ERROR: IDSet.TakeID() error = %v", err)
				}
				time.Sleep(300 * time.Nanosecond)
			}
			outStr = out.String()
			t.Logf("g-%d: push back: \t\t%s", in, outStr)
			err = s.PushIDsFromString(outStr)
			if err != nil {
				t.Errorf("ERROR: idGenerator.PushIDsFromString() error = %v", err)
				return
			}
		}(i, set)
	}
	wg.Wait()
	gotSize := set.GetSize()
	if gotSize != wantSize {
		t.Errorf("ERROR: state after = %v, \n\t\t\t\t\t\t\t want state  = %v", gotSize, wantSize)
	}
	t.Logf("State after: %s", set.String())
	t.Logf("Size after: %v", gotSize)
}

func Test_IDSet_TakeIDSet(t *testing.T) {

	set := NewIDSet([]IDRange{
		NewIDRange(1, 50001, false),
	}, OperationIdCategory, false)
	t.Logf("Before: %s", set.String())

	wg := sync.WaitGroup{}
	const noGoroutines = 10000
	const take = 5
	const wantSize = 50001 - noGoroutines*take

	for i := 0; i < noGoroutines; i++ {
		wg.Add(1)
		go func(in int, s *IDSet) {
			defer wg.Done()

			for j := 0; j < take; j++ {
				_, err := s.TakeID()
				if err != nil {
					t.Errorf("ERROR: IDSet.TakeID() error = %v", err)
					return
				}
			}
		}(i, set)
	}
	wg.Wait()
	t.Logf("After: %s", set.String())
	gotSize := set.GetSize()
	if gotSize != wantSize {
		t.Errorf("ERROR: state after = %v, \n\t\t\t\t\t\t\t want state  = %v", gotSize, wantSize)
	}
}

func Test_IDSet_PushIDs(t *testing.T) {
	wg := sync.WaitGroup{}

	set := NewIDSet([]IDRange{
		NewIDRange(10000001, 10000000, false),
	}, OperationIdCategory, false)

	const take = 100
	const wantSize = 10000000

	for i := 1; i <= wantSize; i += take {
		wg.Add(1)
		go func(in int, w sync.WaitGroup, s *IDSet) {
			defer wg.Done()
			newSet := NewIDSet([]IDRange{
				NewIDRange(uint64(in), uint64(in+take-1), false),
			}, OperationIdCategory, false)

			err := s.PushIDsFromString(newSet.String())
			if err != nil {
				t.Errorf("ERROR: idGenerator.PushIDsFromString() error = %v", err)
				return
			}
		}(i, wg, set)
	}
	wg.Wait()
	gotSize := set.GetSize()
	if gotSize != wantSize {
		t.Errorf("ERROR: size after = %v, \n\t\t\t\t\t\t\t want size  = %v", gotSize, wantSize)
	}
}
