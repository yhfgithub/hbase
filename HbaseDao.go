package dao

import (
"context"
"fmt"
"github.com/astaxie/beego/logs"
"github.com/pkg/errors"
"github.com/sirupsen/logrus"
"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/filter"
	"github.com/tsuna/gohbase/hrpc"
	"io"
	"os"
"github.com/astaxie/beego"
)
var client gohbase.Client

var adminClient gohbase.AdminClient
func init() {
	// 以Stdout为输出，代替默认的stderr
	logrus.SetOutput(os.Stdout)
	// 设置日志等级
	logrus.SetLevel(logrus.DebugLevel)
	ConnectHBase()

}

//建立连接
func ConnectHBase() {
	hbaseUrl := beego.AppConfig.String("hbasehost")
	user := beego.AppConfig.String("userName")
	option := gohbase.EffectiveUser(user)
	client = gohbase.NewClient(hbaseUrl,option)
	adminClient = gohbase.NewAdminClient(hbaseUrl,option)
	fmt.Println("hbase的client连接：",client)
	fmt.Println("hbase的adminClient连接：",adminClient)
}

//创建表
func CreateTable(table string, families map[string]map[string]string){
	ct := hrpc.NewCreateTable(context.Background(), []byte(table),families)
	err := adminClient.CreateTable(ct)
	if err!=nil{
		logs.Error("create ",table," table error:",err)
	}else{
		logs.Info("create ",table," table ok.")
	}
}

//删除表
func DeleteTable(table string){
	dist := hrpc.NewDisableTable(context.Background(), []byte(table))
	err := adminClient.DisableTable(dist)
	if err!=nil{
		logs.Error("disable ",table, " table error",err)
		fmt.Println("disable ",table," table error.")
	}
	dt := hrpc.NewDeleteTable(context.Background(),[]byte(table))
	err = adminClient.DeleteTable(dt)
	if err!=nil{
		logs.Error("delete ",table, " table error",err)
		fmt.Println("delete ",table," table error.")
	}else{
		logs.Info("delete ",table," table ok.")
		fmt.Println("delete ",table," table ok.")
	}
}

//判断表是否存在
func ExistTable(table string)bool{
	ltn,err:=hrpc.NewListTableNames(context.Background())
	if err!=nil{
		logs.Error("NewListTableNames error:",err)
	}
	tns,err2 := adminClient.ListTableNames(ltn)
	if err2!=nil{
		logs.Error("ListTableNames error:",err2)
	}
	for _,tn := range tns{
		if(table==string(tn.Qualifier)){
			return true
		}
	}
	return false
}


//按ROW前缀查询全表所有行。
func ScanRows(table,prefix string)[]string{
	pFilter := filter.NewPrefixFilter([]byte(prefix))
	scanRequest,err:=hrpc.NewScanStr(context.Background(),table,hrpc.Filters(pFilter))
	if err!=nil{
		logs.Error(err)
	}
	scanRsp:= client.Scan(scanRequest)
	strs:=[]string{}
	for(true){
		res_row,err:=scanRsp.Next()
		if(err!=nil||err==io.EOF){
			break
		}
		for _,cell:=range res_row.Cells{
			strs =append(strs,string(cell.Row))
			break
		}
	}
	return strs
}

//查表中该列下的value，有则返回该行信息。
func ScanColumn(table ,column, value string)*hrpc.Result{
	cpFilter := filter.NewColumnPrefixFilter([]byte(column))
	//sc := filter.NewPageFilter()
	scanRequest,err:=hrpc.NewScanStr(context.Background(),table,hrpc.Filters(cpFilter))
	if err!=nil{
		logs.Error(err)
	}
	scanRsp:= client.Scan(scanRequest)
	for(true){
		res_row,err:=scanRsp.Next()
		if(err!=nil||err==io.EOF){
			break
		}
		for _,cell:= range res_row.Cells{
			if string(cell.Value)==value{
				rs,_:=Gets(table,string(cell.Row))
				return rs
			}
		}
	}
	return nil
}



//增添或修改
func PutsByRowkey(table, rowKey string, values map[string]map[string][]byte) (err error) {
	putRequest, err := hrpc.NewPutStr(context.Background(), table, rowKey, values)
	if err != nil {
		logs.Error("hrpc.NewPutStr:", err.Error())
	}
	_, err = client.Put(putRequest)
	if err != nil {
		logs.Error("hbase Put:", err.Error())
	}
	return
}

//获取rowKey的数据集
func Gets(table, rowKey string) (*hrpc.Result, error) {
	getRequest, err := hrpc.NewGetStr(context.Background(), table, rowKey)
	if err != nil {
		logs.Error("hrpc.NewGetStr error:", err.Error())
	}
	res, err := client.Get(getRequest)
	if err != nil {
		logs.Error("hbase clients error: ", err.Error())
	}
	if res.Cells ==nil{
		logs.Error("res.Cells is nil.")
	}
	defer func() {
		if errs := recover(); errs != nil {
			switch fmt.Sprintf("%v", errs) {
			case "runtime error: index out of range":
				err = errors.New("NoSuchRowKeyOrQualifierException")
			case "runtime error: invalid memory address or nil pointer dereference":
				err = errors.New("NoSuchColFamilyException")
			default:
				err = fmt.Errorf("default errot:%v", errs)
			}
		}
	}()
	return res, nil
}
//查看数据集的具体值
func showRowkey(res *hrpc.Result){
	if res.Cells != nil{
		for _,cell := range res.Cells{
			fmt.Print("列族:",string(cell.Family))
			fmt.Print(" 限定符:",string(cell.Qualifier))
			fmt.Println(" value:",string(cell.Value))
		}
	}else{
		logs.Error("res.Cells is nil.")
	}
}

//判断rowkey是否存在
func exist(table, rowKey string) (bool) {
	getRequest, err := hrpc.NewGetStr(context.Background(), table, rowKey)
	if err != nil {
		logs.Error("hrpc.NewGetStr error:", err.Error())
	}
	res, err := client.Get(getRequest)
	if err != nil {
		logs.Error("hbase clients error: ", err.Error())
	}
	if len(res.Cells)>0{
		return true
	}else {
		return false
	}
}

//删除操作
func DeleteByRowkey(table, rowkey string, value map[string]map[string][]byte) (err error) {
	deleteRequest, err := hrpc.NewDelStr(context.Background(), table, rowkey, value)
	if err != nil {
		logs.Error("hrpc.NewDelStrRef: %s", err.Error())
	}
	//fmt.Println("deleteRequest:", deleteRequest)
	res, err := client.Delete(deleteRequest)
	fmt.Println(res)
	if err != nil {
		logs.Error("hrpc.Scan: %s", err.Error())
	}
	return
}
	//
	//value := map[string]map[string][]byte{
	//	"baseInfo":map[string][]byte{
	//		"userId":[]byte("123456"),
	//		"name":[]byte("yanghongfei"),
	//		"age":[]byte("18"),
	//	},
	//	"eat":map[string][]byte{
	//		"food":[]byte("rice"),
	//		"drink":[]byte("water"),
	//	},
	//}
	//PutsByRowkey("Students","rowkey3",value)
	//_ = DeleteByRowkey("Students", "rowkey3", value)

	//family:= map[string]map[string]string{
	//	"baseInfo":map[string]string{
	//		"userId":"",
	//		"name":"",
	//		"age":"",
	//	},
	//	"eat":map[string]string{
	//		"food":"",
	//		"drink":"",
	//	},
	//}

	//CreateTable("webapi",family)
	//if !exist("webapi","rowkey1"){
	//	PutsByRowkey("webapi","rowkey1",value)
	//
	//}



	//if exist("webapi","rowkey1"){
	//	res,err:=Gets("webapi", "rowkey1")
	//	if err!=nil{
	//		logs.Error("res error:",err.Error())
	//	}
	//	showRowkey(res)
	//}


	//fmt.Println(ExistTable("webapi"))
	//fmt.Println("ok")





