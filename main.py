import pymongo,time,threading
from bson import ObjectId

#需要定义的参数
conn_in_ip = 'IP'
conn_in_port = port
conn_out_ip = 'IP'
conn_out_port = port
db_name = "db_name"
collection_name = "collection_name"
#每线程取数据库条数
li = 5000
#最大线程数
max_th = 20

#需要读取的mongodb及集合
conn_in = pymongo.MongoClient(host=conn_in_ip,port=conn_in_port)
collection_in = eval("conn_in." + db_name + "." + collection_name)

#需要写入的mongodb及集合
conn_out = pymongo.MongoClient(host=conn_out_ip,port=conn_out_port)
collection_out = eval("conn_out." + db_name + "." + collection_name)

#日志数据库
log_collection = eval("conn_out.logs." + collection_name)
#错误输出文件
error_log = "%s.txt"%collection_name



#查询最后最大线程数*2的log，线程是异步的，远一点确认，定位从新同步的位置。
log_count = log_collection.count()
if log_count == 0:
    sk = 0
    last_id = 0
else:
    if log_count > max_th * 2:
        log_nu = max_th * 2
    else:
        log_nu = log_count
    log_re = list(log_collection.find())[-log_nu:]
    log_sorted = sorted(log_re, key=lambda re_key: re_key['sk'])
    sk_list = []
    for i in log_sorted:
        sk_list.append(i["sk"])
    for i in log_sorted:
        if i["last_sk"] not in sk_list:
            sk = i["last_sk"]
            last_id = i["last_id"]
            break


#插入方法,后面多线程调用
def m_t_m(re_list,last_id,sk,re_len):
    global th_n
    global count
    # print(re_list)
    for i in re_list:
        try:
            collection_out.insert(i)
        except:
            # 如果出错，判断是不是有这个ID，如果重复就不管，如果没有重复就把错误的id写到一个文件夹里面
            id = i["_id"]
            l = len(list(collection_out.find({"_id": id})))
            if l != 1:
                with open(error_log, "a") as f:
                    f.write(str(id) + '\n')
    #执行完就把当次线程的起始条数、读取的长度、最后的id记录下来，如果程序出错了，可以看哪里不连续，用last_id跑一次
    end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    last_sk = sk + re_len
    log_collection.insert({
        "sk":sk,
        "re_len": re_len,
        "last_sk":last_sk,
        "last_id":last_id,
        "time":end_time
    })
    #把当次线程的起始条数、读取的长度、最后的id，及当前时间输出到控制台
    print(count,sk,re_len,last_sk,last_id,end_time)
    #线程数减一
    th_n -= 1

#定义线程数变量
th_n = 0
#程序开始前，统计需要同步的数据条数，已同步的条数，和最后的id,及当前的时间
count = collection_in.count()
print("count",count,"sk",sk,"last_id",last_id,time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
#一直循环，如果数据跑完了，就等着，一旦有新数据进来就及时同步
while True:
    while sk <count:
        #th_n控制需要启动的线程熟练
        if th_n == max_th:
            time.sleep(3)
            continue
        #首次跑没有last_id
        if not last_id:
            re = collection_in.find().limit(li)
        else:
            re = collection_in.find({'_id':{'$gt':ObjectId(last_id)}}).limit(li)
        re_list = list(re)
        last_id = re_list[-1]["_id"]
        re_len = len(re_list)
        #启动线程
        t = threading.Thread(target=m_t_m,args=(re_list,last_id,sk,re_len))
        t.start()
        th_n += 1
        if count - sk < li:
            sk += count - sk
        else:
            sk += li
        # time.sleep(2)
    if sk == count:
        time.sleep(30)
        count = collection_in.count()
        print(count)
