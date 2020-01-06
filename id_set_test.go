package id_generator

import (
	"reflect"
	"testing"
)

func Test_idSet_GetSize(t *testing.T) {
	type fields struct {
		ranges   []IDRange
		category string
		readOnly bool
	}
	tests := []struct {
		name   string
		fields fields
		want   uint64
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
			want: 3000,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSet(idSet{
				Ranges:   tt.fields.ranges,
				Category: tt.fields.category,
				readOnly: tt.fields.readOnly,
			})
			if got := s.GetSize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("idSet.getSizeCh() = %v, wantOut %v", got, tt.want)
			}
		})
	}
}

func Test_idSet_TakeFirstRange(t *testing.T) {
	type fields struct {
		ranges   []IDRange
		category string
		readOnly bool
	}
	tests := []struct {
		name      string
		fields    fields
		wantOut   IDRange
		wantState uint64
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
			wantOut:   NewIDRange(1, 1000, false),
			wantState: 1001,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := idSet{
				Ranges:   tt.fields.ranges,
				Category: tt.fields.category,
				readOnly: tt.fields.readOnly,
			}
			got := s.takeFirstRange()
			if got.getStartID() != tt.wantOut.getStartID() {
				t.Errorf("OUT: CurrentStartID = %d, wantOut %d",
					got.getStartID(), tt.wantOut.getStartID())
				return
			}
			if got.getEndID() != tt.wantOut.getEndID() {
				t.Errorf("OUT: EndID = %d, wantOut %d",
					got.getEndID(), tt.wantOut.getEndID())
				return
			}
			if !reflect.DeepEqual(s.Ranges[0].getStartID(), tt.wantState) {
				t.Errorf("STATE: s.Ranges.CurrentStartID = %d, wantState.CurrentStartID = %d",
					s.Ranges[0].getStartID(), tt.wantState)
			}
		})
	}
}

func Test_idSet_takeID(t *testing.T) {
	type fields struct {
		Ranges   idRanges
		Category string
		ReadOnly bool
	}
	tests := []struct {
		name      string
		fields    fields
		wantOut   uint64
		wantState idSet
		wantErr   bool
	}{
		{
			name: "PUSH_IDS",
			fields: fields{
				Ranges: []IDRange{
					NewIDRange(2001, 3000, false),
					NewIDRange(4001, 5000, false),
				},
			},
			wantOut: 2001,
			wantState: newIDSet([]IDRange{
				NewIDRange(2002, 3000, false),
				NewIDRange(4001, 5000, false),
			}, "", false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &idSet{
				Ranges:   tt.fields.Ranges,
				Category: tt.fields.Category,
				readOnly: tt.fields.ReadOnly,
			}
			got, err := s.takeID()
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.takeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.wantOut) {
				t.Errorf("idSet.takeID() = %v, wantOut %v", got, tt.wantOut)
				return
			}
			ss := s.toString()
			ts := tt.wantState.toString()
			if ss != ts {
				t.Errorf("\ntakeID()  = %v, \nwantState = %v", ss, ts)
			}
		})
	}
}

func Test_idSet_peekNextID(t *testing.T) {
	type fields struct {
		Ranges   idRanges
		Category string
		ReadOnly bool
	}
	tests := []struct {
		name      string
		fields    fields
		wantOut   uint64
		wantState idSet
		wantErr   bool
	}{
		{
			name: "PUSH_IDS",
			fields: fields{
				Ranges: []IDRange{
					NewIDRange(2001, 3000, false),
					NewIDRange(4001, 5000, false),
				},
			},
			wantOut: 2001,
			wantState: newIDSet([]IDRange{
				NewIDRange(2001, 3000, false),
				NewIDRange(4001, 5000, false),
			}, "", false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &idSet{
				Ranges:   tt.fields.Ranges,
				Category: tt.fields.Category,
				readOnly: tt.fields.ReadOnly,
			}
			got, err := s.peekNextID()
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.takeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.wantOut) {
				t.Errorf("idSet.takeID() = %v, wantOut %v", got, tt.wantOut)
				return
			}
			ss := s.toString()
			ts := tt.wantState.toString()
			if ss != ts {
				t.Errorf("\ntakeID()  = %v, \nwantState = %v", ss, ts)
			}
		})
	}
}

func Test_idSet_String(t *testing.T) {
	type fields struct {
		Ranges   idRanges
		Category string
		ReadOnly bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "STRING",
			fields: fields{
				Ranges: idRanges{
					{
						CurrentStartID: 1,
						EndID:          1000,
					},
				},
				Category: OperationIdCategory,
				ReadOnly: false,
			},
			want: "{\"ranges\":[{\"currentStartID\":1,\"endID\":1000}],\"category\":\"ewa_operation_uid\"}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &idSet{
				Ranges:   tt.fields.Ranges,
				Category: tt.fields.Category,
				readOnly: tt.fields.ReadOnly,
			}
			if got := s.toString(); got != tt.want {
				t.Errorf("idSet.toString() = %v, wantOut %v", got, tt.want)
			}
		})
	}
}

func Test_setFromString(t *testing.T) {
	type args struct {
		jsn string
	}
	tests := []struct {
		name    string
		args    args
		want    idSet
		wantErr bool
	}{
		{
			name: "RANGES_FROM_STRING",
			args: args{
				jsn: "{\"ranges\":[{\"currentStartID\":1,\"endID\":1000}],\"category\":\"ewa_operation_uid\"}",
			},
			want: idSet{
				Ranges: idRanges{
					{
						CurrentStartID: 1,
						EndID:          1000,
					},
				},
				Category: OperationIdCategory,
				readOnly: false,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := setFromString(tt.args.jsn)
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.takeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setFromString() = %v, wantOut %v", got.toString(), tt.want.toString())
			}
			t.Log(got)
		})
	}
}

func Test_idSet_pushIDs(t *testing.T) {
	type fields struct {
		ranges   idRanges
		category string
		readOnly bool
	}
	type args struct {
		pushedIDSet idSet
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantState idSet
		wantErr   bool
	}{
		{
			name: "PUSH_IDS",
			fields: fields{
				ranges: []IDRange{
					NewIDRange(1, 1000, false),
					NewIDRange(2001, 3000, false),
					NewIDRange(4001, 5000, false),
				},
				category: OperationIdCategory,
			},
			args: args{
				pushedIDSet: newIDSet([]IDRange{
					NewIDRange(1001, 2000, false),
					NewIDRange(3001, 4000, false),
					NewIDRange(5001, 6000, false),
				}, OperationIdCategory, false),
			},
			wantState: newIDSet([]IDRange{
				NewIDRange(1, 6000, false),
			}, OperationIdCategory, false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &idSet{
				Ranges:   tt.fields.ranges,
				Category: tt.fields.category,
				readOnly: tt.fields.readOnly,
			}
			err := s.pushIDs(tt.args.pushedIDSet)
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.pushIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			rs := s.toString()
			ws := tt.wantState.toString()
			if rs != ws {
				t.Errorf("idSet.pushIDs(): \n\tstate \t  %v, \n\twantState %v", rs, ws)
			}
		})
	}
}

func Test_IDSet_pushIDs(t *testing.T) {
	set := NewIDSet([]IDRange{
		NewIDRange(10000001, 10000000, false),
	}, OperationIdCategory, false)

	const take = 100
	const wantSize = 10000000

	for i := 1; i <= 10000000; i += take {
		newSet := NewIDSet([]IDRange{
			NewIDRange(uint64(i), uint64(i+take-1), false),
		}, OperationIdCategory, false)

		err := set.PushIDsFromString(newSet.String())
		if err != nil {
			t.Errorf("ERROR: idGenerator.PushIDsFromString() error = %v", err)
			return
		}
	}
	gotSize := set.GetSize()
	if gotSize != wantSize {
		t.Errorf("ERROR: size after = %v, \n\t\t\t\t\t\t\t want size  = %v", gotSize, wantSize)
	}
}

func Test_idSet_takeIDs(t *testing.T) {
	type fields struct {
		Ranges   idRanges
		Category string
		readOnly bool
	}
	type args struct {
		setSize uint64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantOut   idSet
		wantState idSet
		wantErr   bool
	}{
		{
			name: "TAKE_IDS",
			fields: fields{
				Ranges: []IDRange{
					NewIDRange(1, 1000, false),
					NewIDRange(2001, 3000, false),
					NewIDRange(4001, 5000, false),
				},
				Category: OperationIdCategory,
			},
			args: args{
				setSize: 100,
			},
			wantOut: newIDSet([]IDRange{
				NewIDRange(1, 100, false),
			}, OperationIdCategory, false),
			wantState: newIDSet([]IDRange{
				NewIDRange(101, 1000, false),
				NewIDRange(2001, 3000, false),
				NewIDRange(4001, 5000, false),
			}, OperationIdCategory, false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &idSet{
				Ranges:   tt.fields.Ranges,
				Category: tt.fields.Category,
				readOnly: tt.fields.readOnly,
			}
			got, err := s.takeIDs(tt.args.setSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("idSet.takeIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.wantOut) {
				t.Errorf("idSet.takeIDs() = %v, wantOut %v", got, tt.wantOut)
			}
			if !reflect.DeepEqual(s.getSize(), tt.wantState.getSize()) {
				t.Errorf("idSet.takeIDs() = %v, wantState %v", s.getSize(), tt.wantState.getSize())
			}
		})
	}
}
