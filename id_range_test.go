package id_generator

import (
	"testing"
)

func Test_idRange_TakeIDs(t *testing.T) {
	type fields struct {
		currentStartID uint64
		endID          uint64
		readOnly       bool
	}
	type args struct {
		idRangeSize uint64
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantOut   IDRange
		wantState IDRange
		wantErr   bool
	}{
		{
			name: "TAKE_RANGE_ONE",
			fields: fields{
				currentStartID: 1,
				endID:          10000,
			},
			args: args{
				1,
			},
			wantOut:   NewIDRange(1, 1, false),
			wantState: NewIDRange(2, 10000, false),
			wantErr:   false,
		},
		{
			name: "TAKE_RANGE_THOUSAND",
			fields: fields{
				currentStartID: 1,
				endID:          10000,
			},
			wantOut:   NewIDRange(1, 1000, false),
			wantState: NewIDRange(1001, 10000, false),
			wantErr:   false,
			args: args{
				1000,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := IDRange{
				CurrentStartID: tt.fields.currentStartID,
				EndID:          tt.fields.endID,
				readOnly:       tt.fields.readOnly,
			}
			got, err := i.takeIDs(tt.args.idRangeSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("IDRange.takeIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}
			if got.getStartID() != tt.wantOut.getStartID() {
				t.Errorf("OUT: CurrentStartID = %d, wantOut %d",
					got.getStartID(), tt.wantOut.getStartID())
			}
			if got.getEndID() != tt.wantOut.getEndID() {
				t.Errorf("OUT: EndID = %d, wantOut %d",
					got.getEndID(), tt.wantOut.getEndID())
			}

			if i.getStartID() != tt.wantState.getStartID() {
				t.Errorf("STATE: CurrentStartID = %d, wantState %d",
					i.getStartID(), tt.wantState.getStartID())
			}
			if i.getEndID() != tt.wantState.getEndID() {
				t.Errorf("STATE: EndID = %d, wantState %d",
					i.getEndID(), tt.wantState.getEndID())
			}
		})
	}
}

func Test_idRange_TakeID(t *testing.T) {
	type fields struct {
		currentStartID uint64
		endID          uint64
		readOnly       bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "take_1",
			fields: fields{
				currentStartID: 1,
				endID:          10000,
			},
			wantErr: false,
		},
	}
	tt := tests[0]
	i := IDRange{
		CurrentStartID: tt.fields.currentStartID,
		EndID:          tt.fields.endID,
		readOnly:       tt.fields.readOnly,
	}
	for j := 1; j <= 100; j++ {
		t.Run(tt.name, func(t *testing.T) {

			got, err := i.takeID()
			if (err != nil) != tt.wantErr {
				t.Errorf("IDRange.takeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == 0 {
				return
			}
			want := uint64(j)
			wantState := uint64(j + 1)
			if got != want {
				t.Errorf("OUT: NewID = %v, wantOut %v", got, want)
			}
			if i.getStartID() != wantState {
				t.Errorf("STATE: CurrentStartID = %v, wantState %v", i.getStartID(), wantState)
			}
		})
	}
}

func Test_idRange_Merge(t *testing.T) {
	type fields struct {
		currentStartID uint64
		endID          uint64
		readOnly       bool
	}
	type args struct {
		newRange IDRange
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantOut   IDRange
		wantState IDRange
	}{
		{
			name: "MERGE",
			fields: fields{
				currentStartID: 1,
				endID:          1000,
			},
			args: args{
				newRange: NewIDRange(1001, 2000, false),
			},
			wantOut:   NewIDRange(1, 2000, false),
			wantState: NewIDRange(1, 1000, false),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := IDRange{
				CurrentStartID: tt.fields.currentStartID,
				EndID:          tt.fields.endID,
				readOnly:       tt.fields.readOnly,
			}
			got := i.tryMerge(tt.args.newRange)
			if got.CurrentStartID != tt.wantOut.CurrentStartID {
				t.Errorf("OUT: CurrentStartID = %d, wantOut %d",
					got.CurrentStartID, tt.wantOut.CurrentStartID)
			}
			if got.EndID != tt.wantOut.EndID {
				t.Errorf("OUT: EndID = %d, wantOut %d",
					got.EndID, tt.wantOut.EndID)
			}

			if i.CurrentStartID != tt.wantState.CurrentStartID {
				t.Errorf("STATE: CurrentStartID = %d, wantState %d",
					i.CurrentStartID, tt.wantState.CurrentStartID)
			}
			if i.EndID != tt.wantState.EndID {
				t.Errorf("STATE: EndID = %d, wantState %d",
					i.EndID, tt.wantState.EndID)
			}
		})
	}
}
