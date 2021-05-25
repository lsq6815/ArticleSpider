# 消息队列的名字
QUEUE_NAME = 'task_queue'
# 保存内存中的数据的阈值，超过这个阈值就会保存所有数据到外存
THRESHOLD = 10
# 入口地址
ENTRY_URL = "http://society.people.com.cn/GB/index.html"
# 限定爬取的 Domain（人民网通过 Domain 前缀区分频道）
ALLOWED_DOMAINS = {
    'society.people.com.cn',  # 社会
    'legal.people.com.cn',   # 法制
}
# 最大线程数
MAX_THREADS = 30
# 图片的存储路径
IMAGE_DIR = "images"

# Mongodb hostname
MONGODB_HOSTNAME = 'localhost'
# Mongodb port
MONGODB_PORT = 27017
# Mongodb database
MONGODB_DB = 'scraper'
# Mongodb collection
MONGODB_COLLECTION = 'peoplecomcn'
