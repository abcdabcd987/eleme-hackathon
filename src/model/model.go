package model

import (
	"../constant"
	"database/sql"
	//"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/redis.v3"
	"log"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"time"
)

/** random string **/
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var src = rand.NewSource(time.Now().UnixNano())

func RandString(n int) string {
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

/** random string **/

var L = log.New(os.Stderr, "", 0)
var r = redis.NewClient(&redis.Options{
	Addr:     os.Getenv("REDIS_HOST") + ":" + os.Getenv("REDIS_PORT"),
	Password: "",
	DB:       0,
})

type userType struct {
	id, name, password string
}

type cartType struct {
	food_id string
	count int
}

var cache_user = make(map[string]userType) //token -> UserType
var cache_userid = make(map[string]string) //name -> id
var cache_user_order = make(map[string]string)
var cache_food_price = make(map[string]int)
var cache_food_stock = make(map[string]int)
var cache_token_user = make(map[string]string)
var cache_food_last_update_time int
var cache_cart_user = make(map[string]string)
var cache_cart = make(map[string][]cartType)
var cache_order_cart = make(map[string]string)

var addFood, queryStock, placeOrder *redis.Script
func Load_script_from_file(filename string) *redis.Script {
	   command_raw, err := ioutil.ReadFile(filename)
	   if err != nil {
			   L.Fatal("Failed to load script " + filename)
	   }
	   command := string(command_raw)
	   //return r.ScriptLoad(command).Val()
	   return redis.NewScript(command)
}

func atoi(str string) int {
	res, err := strconv.Atoi(str)
	if err != nil {
		L.Panic(err)
	}
	return res
}

func PostLogin(username string, password string) (int, string, string) {
	user_id, ok := cache_userid[username]
	if !ok {
		return -1, "", ""
	}

	password_ := cache_user[user_id].password
	if password != password_ {
		return -1, "", ""
	}

	token := RandString(8)
	cache_token_user[token] = user_id
	return 0, user_id, token
}

func get_token_user(token string) string {
	if id, ok := cache_token_user[token]; ok {
		return id
	} else {
		return ""
	}
}

func Is_token_exist(token string) bool {
	if nid := get_token_user(token); nid == "" {
		return false
	} else {
		return true
	}
}

func Create_cart(token string) string {
	cartid := RandString(32)
	cache_cart_user[cartid] = get_token_user(token)
	return cartid
}

func Cart_add_food(token, cartid string, foodid int, count int) int {
	foodid_s := strconv.Itoa(foodid)
	_, exist := cache_food_price[foodid_s]
	if !exist {
		return -2
	}
	belong_user := cache_token_user[token]
	cart_user, ok := cache_cart_user[cartid]
	if !ok {
		return -1
	}
	if cart_user != belong_user {
		return -4
	}
	sum := 0
	for _, v := range cache_cart[cartid] {
		sum += v.count
	}
	if sum + count > 3 {
		return -3
	}
	cache_cart[cartid] = append(cache_cart[cartid], cartType{foodid_s, count})
	return 0
}

func Get_foods() []map[string]interface{} {
	var ret []map[string]interface{}
	for k, _ := range cache_food_price {
		food_id, _ := strconv.Atoi(k)
		ret = append(ret, map[string]interface{}{
			"id":    food_id,
			"price": cache_food_price[k],
			"stock": cache_food_stock[k],
		})
	}
	return ret
}

func PostOrder(cart_id string, token string) (int, string) {
	order_id := RandString(8)

	user_id, _ := cache_token_user[token]
	belong_user, ok2 := cache_cart_user[cart_id]
	user_order, _ := cache_user_order[user_id]
	if !ok2 {
		return -1, order_id
	}
	if user_id != belong_user {
		return -2, order_id
	}
	if user_order != "" {
		return -4, order_id
	}

	for _, v := range cache_cart[cart_id] {
		if cache_food_stock[v.food_id] < v.count {
			return -3, order_id
		}
	}
	for _, v := range cache_cart[cart_id] {
		cache_food_stock[v.food_id] -= v.count
	}
	cache_user_order[user_id] = order_id
	cache_order_cart[order_id] = cart_id

	return 0, order_id
}

func GetOrder(token string) (ret map[string]interface{}, found bool) {
	userid := get_token_user(token)
	uid, _ := strconv.Atoi(userid)
	orderid := cache_user_order[userid]
	if orderid == "" {
		found = false
		return
	}
	found = true
	cartid := cache_order_cart[orderid]
	var item_arr []map[string]int
	total := 0
	for _, v := range cache_cart[cartid] {
		food := v.food_id
		f, _ := strconv.Atoi(food)
		c := v.count
		price := cache_food_price[food]
		total += price * c
		item_arr = append(item_arr, map[string]int{"food_id": f, "count": c})
	}
	ret = map[string]interface{}{
		"userid":  uid,
		"orderid": orderid,
		"items":   item_arr,
		"total":   total,
	}
	return
}

/** init code **/

func init_cache_and_redis(init_redis bool) {
	L.Print("Actual init begins, init_redis=", init_redis)
	addFood = Load_script_from_file("src/model/lua/add_food.lua")
	queryStock = Load_script_from_file("src/model/lua/query_stock.lua")
	placeOrder = Load_script_from_file("src/model/lua/place_order.lua")
	cache_food_last_update_time = 0
	db, dberr := sql.Open("mysql",
		os.Getenv("DB_USER")+
			":"+
			os.Getenv("DB_PASS")+
			"@tcp("+
			os.Getenv("DB_HOST")+
			":"+
			os.Getenv("DB_PORT")+
			")/"+
			os.Getenv("DB_NAME"))
	defer db.Close()
	if dberr != nil {
		L.Fatal(dberr)
	}

	if init_redis {
		r.FlushAll()
		r.ScriptFlush()
	}

	now := 0
	rows, _ := db.Query("SELECT id,name,password from user")
	for rows.Next() {
		var id, name, pwd string
		rows.Scan(&id, &name, &pwd)
		cache_userid[name] = id
		cache_user[id] =
			userType{
				id:       id,
				name:     name,
				password: pwd,
			}
	}

	rows, _ = db.Query("SELECT id,stock,price from food")
	p := r.Pipeline()
	for rows.Next() {
		var id string
		var stock, price int
		rows.Scan(&id, &stock, &price)
		idInt := atoi(id)
		now += 1
		cache_food_price[id] = price
		cache_food_stock[id] = stock
		L.Print("adding food:", id)
		if init_redis {
			p.ZAdd(constant.FOOD_STOCK_KIND,
				redis.Z{
					float64(now),
					now*constant.TIME_BASE + idInt,
				})
			p.ZAdd(constant.FOOD_STOCK_COUNT,
				redis.Z{
					float64(now),
					now*constant.TIME_BASE + stock,
				})
			p.HSet(constant.FOOD_LAST_UPDATE_TIME, id, strconv.Itoa(now))
		}
	}
	if init_redis {
		p.Set(constant.TIMESTAMP, now, 0)
		p.Set(constant.INIT_TIME, -10000, 0)
		p.Exec()
	}

}

func Sync_redis_from_mysql() {
	if constant.DEBUG {
		r.Del(constant.INIT_TIME)
	}

	if r.Incr(constant.INIT_TIME).Val() == 1 {
		L.Println("Ready to init redis")
		init_cache_and_redis(true)
	} else {
		L.Println("Already been init")
		init_cache_and_redis(false)
		for atoi(r.Get(constant.INIT_TIME).Val()) >= 1 {
			time.Sleep(200 * time.Millisecond)
		}
	}
}

/** init code **/
