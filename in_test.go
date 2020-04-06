package sqlx

import (
	"reflect"
	"testing"
)

func TestIn2(t *testing.T) {
	type args struct {
		query string
		args  []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		want1   []interface{}
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				query: "select * from T where a=?,b=?,c=?",
				args: []interface{}{
					1, 2, 3,
				},
			},
			want: "select * from T where a=?,b=?,c=?",
			want1: []interface{}{
				1, 2, 3,
			},
			wantErr: false,
		},
		{
			name: "2",
			args: args{
				query: "select * from T where a=?,b in(?),c in(?)",
				args: []interface{}{
					1, []int{2}, []int{3},
				},
			},
			want: "select * from T where a=$1,b in($2),c in($3)",
			want1: []interface{}{
				1, 2, 3,
			},
			wantErr: false,
		},
		{
			name: "3",
			args: args{
				query: "select * from T where a=?,b in($2),c in($2)",
				args: []interface{}{
					1, []int{2, 3},
				},
			},
			want: "select * from T where a=$1,b in($2,$3),c in($2,$3)",
			want1: []interface{}{
				1, 2, 3,
			},
			wantErr: false,
		},
		{
			name: "4",
			args: args{
				query: "select * from T where a=?,b=$1,c IN($2),d=$3,e IN($2)",
				args: []interface{}{
					1, []int{2, 3, 4}, 5,
				},
			},
			want: "select * from T where a=$1,b=$1,c IN($2,$3,$4),d=$5,e IN($2,$3,$4)",
			want1: []interface{}{
				1, 2, 3, 4, 5,
			},
			wantErr: false,
		},
		{
			name: "5",
			args: args{
				query: "select * from T where a=?,b=$1,c IN($2),d=$3 and ''::jsonb ?? ''",
				args: []interface{}{
					1, []int{2, 3, 4}, 5,
				},
			},
			want: "select * from T where a=$1,b=$1,c IN($2,$3,$4),d=$5 and ''::jsonb ?? ''",
			want1: []interface{}{
				1, 2, 3, 4, 5,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := In(tt.args.query, tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("In() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("In() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("In() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
