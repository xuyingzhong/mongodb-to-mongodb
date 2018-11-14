import pymongo,time,threading
from bson import ObjectId

#需要读取的mongodb及集合
connin = pymongo.MongoClient(host='IP',port=27018)
dbin = connin.ditasdb
collectionin = dbin.test

#需要写入的mongodb及集合
connout = pymongo.MongoClient(host='IP',port=27018)
dbout = connout.ditasdb
collectionout = dbout.test

#插入方法,后面多线程调用
def m_t_m(re_list,last_id,sk,re_len):
    global th_n
    global count
    # print(re_list)
    for i in re_list:
        try:
            collectionout.insert(i)
        except:
            # 如果出错，判断是不是有这个ID，如果重复就不管，如果没有重复就把错误的id写到一个文件夹里面
            id = i["_id"]
            l = len(list(collectionout.find({"_id": id})))
            if l != 1:
                with open('test.txt', "a") as f:
                    f.write(str(id) + '\n')
    #执行完就把当次线程的起始条数、读取的长度、最后的id记录下来，如果程序出错了，可以看哪里不连续，用last_id跑一次
    connout.logs.test.insert({
        "sk":sk,
        "re_len": re_len,
        "last_id":last_id,
    })
    #把当次线程的起始条数、读取的长度、最后的id，及当前时间输出到控制台
    print(sk,re_len,count,last_id)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    #线程数减一
    th_n -= 1


sk =0
li = 5000
th_n = 0
last_id = 0
count = collectionin.count()
print(count)
#一直循环，如果数据跑完了，就等着，一旦有新数据进来就及时同步
while True:
    while sk <count:
        #th_n控制需要启动的线程熟练
        if th_n == 20:
            time.sleep(3)
            continue
        #首次跑没有last_id
        if not last_id:
            re = collectionin.find().limit(li)
        else:
            re = collectionin.find({'_id':{'$gt':ObjectId(last_id)}}).limit(li)
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
        count = collectionin.count()
        print(count)
