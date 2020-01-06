// +build !race

package generator

import (
	"fmt"
	"testing"

	"git.fxclub.org/wallet/id-generator/domain"
	"git.fxclub.org/wallet/id-generator/provider"
)

func Test_idGenerator_TakeIDsWithRetry(t *testing.T) {
	type args struct {
		category string
	}
	tests := []struct {
		name         string
		args         args
		wantTakenIDs *domain.IDSet
		wantState    *domain.IDSet
		wantErr      bool
	}{
		{
			name: "TAKE_FIRST_100_IDS",
			args: args{
				category: domain.OperationIdCategory,
			},
			wantTakenIDs: domain.NewIDSet([]domain.IDRange{
				domain.NewIDRange(1, DefaultIDSetSize, false),
			}, domain.OperationIdCategory, false),
			wantState: domain.NewIDSet([]domain.IDRange{
				domain.NewIDRange(DefaultIDSetSize + 1, defaultTotalSize, false),
			}, domain.OperationIdCategory, false),
		},
		{
			name: "TAKE_NEXT_100_IDS",
			args: args{
				category: domain.OperationIdCategory,
			},
			wantTakenIDs: domain.NewIDSet([]domain.IDRange{
				domain.NewIDRange(DefaultIDSetSize + 1, DefaultIDSetSize * 2, false),
			}, domain.OperationIdCategory, false),
			wantState: domain.NewIDSet([]domain.IDRange{
				domain.NewIDRange(DefaultIDSetSize * 2 + 1, defaultTotalSize, false),
			}, domain.OperationIdCategory, false),
		},
	}
	idP := provider.NewMockIDProvider()
	g := NewIDGenerator(idP)
	if err := g.Initialize(domain.OperationIdCategory, 1); err != nil {
		t.Errorf("generator.Initialize() error = %v", err)
		return
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			gotTakenIDs, err := g.TakeIDsWithRetry(tt.args.category)
			if gotTakenIDs == nil {
				return
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("IDGenerator.TakeIDsWithRetry() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			taken := gotTakenIDs.String()
			want := tt.wantTakenIDs.String()
			if taken != want {
				t.Errorf("IDGenerator.TakeIDsWithRetry() = %v, want %v", taken, want)
			}
			s, err := g.PeekIDs(domain.OperationIdCategory)
			if err != nil {
				t.Errorf("IDGenerator.TakeIDsWithRetry() error = %v", err)
				return
			}
			state := s.String()
			wantState := tt.wantState.String()
			if state != wantState {
				t.Errorf("IDGenerator.TakeIDsWithRetry() = %v, wantState %v", state, wantState)
			}
			t.Log(gotTakenIDs.String())
			set, _ := g.PeekIDs(domain.OperationIdCategory)
			t.Log(set.String())
		})
	}
}

func Test_idGenerator_PeekIDs(t *testing.T) {
	type fields struct {
		idProvider IDProvider
	}
	type args struct {
		category string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *domain.IDSet
		wantErr bool
	}{
		{
			name: "PEEK_IDS",
			fields: fields{
				idProvider: provider.NewMockIDProvider(),
			},
			args: args{
				category: domain.OperationIdCategory,
			},
			want: domain.NewIDSet([]domain.IDRange{
				domain.NewIDRange(1, defaultTotalSize, false),
			}, domain.OperationIdCategory, false),
		},
	}

	for _, tt := range tests {
		idP := provider.NewMockIDProvider()
		g := NewIDGenerator(idP)
		if err := g.Initialize(domain.OperationIdCategory, 1); err != nil {
			t.Errorf("IDGenerator.Initialize() error = %v", err)
			return
		}

		t.Run(tt.name, func(t *testing.T) {
			got, err := g.PeekIDs(tt.args.category)
			if (err != nil) != tt.wantErr {
				t.Errorf("IDGenerator.PeekIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				return
			}
			gs := got.String()
			ws := tt.want.String()
			if gs != ws {
				t.Errorf("IDGenerator.PeekIDs() = %v, want %v", gs, ws)
			}
		})
	}
}

func Test_idGenerator_TakeAndPushIDsWithRetry(t *testing.T) {
	set := domain.NewIDSet([]domain.IDRange{
		domain.NewIDRange(1, defaultTotalSize, false),
	}, domain.OperationIdCategory, false)
	idP := provider.NewMockIDProvider()
	if err := idP.Initialize(set.String(), domain.OperationIdCategory); err != nil {
		t.Errorf("idProvider.Initialize() error = %v", err)
		return
	}

	for i := 0; i < 10000; i += DefaultIDSetSize {

		g := NewIDGenerator(idP)

		t.Log(" ================================================================================================")
		stateBefore, err := g.PeekIDs(domain.OperationIdCategory)
		if err != nil {
			t.Errorf("ERROR: IDGenerator.TakeIDsWithRetry() error = %v", err)
			return
		}
		stateBeforeStr := stateBefore.String()
		t.Log("state before take:", stateBeforeStr)

		wantOut := domain.NewIDSet([]domain.IDRange{
			domain.NewIDRange(uint64(1+i/2), uint64(DefaultIDSetSize+i/2), false),
		}, domain.OperationIdCategory, false)

		wantState := domain.NewIDSet([]domain.IDRange{
			domain.NewIDRange(uint64(DefaultIDSetSize+1+i/2), defaultTotalSize, false),
		}, domain.OperationIdCategory, false)

		out, err := g.TakeIDsWithRetry(domain.OperationIdCategory)
		if err != nil {
			t.Errorf("ERROR: IDGenerator.TakeIDsWithRetry() error = %v", err)
			return
		}
		if out == nil {
			return
		}
		outStr := out.String()
		wantOutStr := wantOut.String()
		if outStr != wantOutStr {
			t.Errorf("ERROR: take out =  %v, \n\t\t\t\t\t\t\t want out =\t %v", outStr, wantOutStr)
			return
		}
		stateAfter, err := g.PeekIDs(domain.OperationIdCategory)
		if err != nil {
			t.Errorf("ERROR: IDGenerator.TakeIDsWithRetry() error = %v", err)
			return
		}
		stateAfterStr := stateAfter.String()
		wantStateStr := wantState.String()
		if stateAfterStr != wantStateStr {
			t.Errorf("ERROR: state after take = %v, \n\t\t\t\t\t   want state after take  = %v", stateAfterStr, wantState)
			return
		}
		t.Log("take out:         ", outStr)
		t.Log("state after take: ", stateAfterStr)
		stateBefore, err = g.PeekIDs(domain.OperationIdCategory)
		if err != nil {
			t.Errorf("ERROR: IDGenerator.PeekIDsWithRetry() error = %v", err)
			return
		}
		t.Log("take 50 IDs ...")
		var lastID uint64
		for j := 0; j < DefaultIDSetSize/2; j++ {
			lastID, err = out.TakeID()
			if err != nil {
				t.Errorf("ERROR: idSet.TakeID() error = %v", err)
				return
			}
		}
		t.Log("================================================================================================")
		stateBeforeStr = stateBefore.String()
		t.Log("state before push:", stateBeforeStr)
		outStr = out.String()
		t.Log("push back: \t\t", outStr)

		wantState = domain.NewIDSet([]domain.IDRange{
			domain.NewIDRange(lastID+1, defaultTotalSize, false),
		}, domain.OperationIdCategory, false)

		_, err = g.PushIDsWithRetry(domain.OperationIdCategory)
		if err != nil {
			t.Errorf("ERROR: IDGenerator.PushIDsWithRetry() error = %v", err)
			return
		}
		stateAfter, err = g.PeekIDs(domain.OperationIdCategory)
		if err != nil {
			t.Errorf("ERROR: IDGenerator.PeekIDs() error = %v", err)
			return
		}
		stateAfterStr = stateAfter.String()
		wantStateStr = wantState.String()
		if stateAfterStr != wantStateStr {
			t.Errorf("ERROR: state after push = %v, \n\t\t\t\t\t\t\t\t\t  wantState %v", stateAfterStr, wantState)
			return
		}
		t.Log(fmt.Sprintf("state after push:  %s\n", stateAfterStr))
	}
}
