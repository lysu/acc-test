package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const srcAcc = 0

func main() {
	db, err := sql.Open("mysql", "Kroot@tcp(127.0.0.1:4000)/test")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	accNum, concurrent, totalMoney := 10000, 16, int64(4000)

	initAccount(db, totalMoney, accNum, err)

	perWorker := accNum / concurrent
	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrent)
	var workers []*worker
	for i := 0; i < concurrent; i++ {
		w := &worker{
			no: i,
			wg: &wg,
			db: db,
			outAcc: struct {
				start int
				end   int
			}{i*perWorker + 1, (i + 1) * perWorker},
		}
		workers = append(workers, w)
		go w.start()
	}

	wg.Wait()
	fmt.Println("time used:", time.Since(start))
	var totalRetry int
	for i := 0; i < concurrent; i++ {
		totalRetry += workers[i].retryCount
	}
	fmt.Println("retry count:", totalRetry)
}

type worker struct {
	no     int
	wg     *sync.WaitGroup
	db     *sql.DB
	outAcc struct {
		start int
		end   int
	}
	retryCount int
}

func (w *worker) start() {
	fmt.Println("start worker:", w.no, w.outAcc.start, w.outAcc.end)
	i := w.outAcc.start
	for {
		done, err := w.transferTo(i)
		if err != nil {
			//fmt.Println(w.no, "----->", fmt.Sprintf("%+v", err))
			if strings.Contains(err.Error(), "try again later") || strings.Contains(err.Error(), "can not retry") {
				//fmt.Println(err)
				//time.Sleep(time.Millisecond * 2)
				w.retryCount++
				continue
			}
			panic(err)
		}
		if done {
			break
		}
		i++
		if i > w.outAcc.end {
			i = w.outAcc.start
		}
	}

	w.wg.Done()
}

func (w *worker) transferTo(target int) (done bool, err error) {
	//fmt.Println("worker", w.no, "transfer 1 to ", target)
	//t := time.Now()
	tx, err := w.db.Begin()
	if err != nil {
		return false, err
	}
	result, err := tx.Query("select id, money from acc where id = ? for update", srcAcc)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	var currentMoney int64
	for result.Next() {
		var (
			id int
			m  int64
		)
		err = result.Scan(&id, &m)
		if err != nil {
			result.Close()
			tx.Rollback()
			return false, err
		}
		if id == 0 {
			currentMoney = m
		}
	}
	result.Close()
	if currentMoney < 1 {
		fmt.Println("worker", w.no, "finished")
		tx.Rollback()
		return true, nil
	}

	//fmt.Println("worker", w.no, "get lock", currentMoney)

	_, err = tx.Exec("update acc set money = money + 1 where id = ?", target)
	if err != nil {
		tx.Rollback()
		return false, err
	}

	_, err = tx.Exec("update acc set money = money - 1 where id = ?", srcAcc)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	err = tx.Commit()
	if err != nil {
		return false, err
	}
	//fmt.Println("tx time:", time.Since(t))
	return false, nil
}

func initAccount(db *sql.DB, totalMoney int64, accNum int, err error) {
	var vs strings.Builder
	vs.WriteString("(0," + strconv.FormatInt(totalMoney, 10) + ")")
	for i := 1; i < accNum; i++ {
		vs.WriteString(", (" + strconv.Itoa(i) + ",0)")
	}
	db.Exec("DROP TABLE `acc`")
	db.Exec("CREATE TABLE `acc` (`id` int, `money` bigint, PRIMARY KEY (`id`) )")
	_, err = db.Exec("INSERT INTO `acc` (id, money) values" + vs.String())
	if err != nil {
		panic(err)
	}
}
