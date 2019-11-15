import pymysql.cursors

import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='xxtxxthmhxb0643',
                             db='flink_test',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

with connection.cursor() as cursor:
    # Read a single record
    sql = "SELECT `startTime`, `endTime`, `category_id`, `sales_volume` FROM `sales_volume_table` WHERE " \
          "`startTime`=%s"

    plt.ion()  # 开启interactive mode 成功的关键函数
    fig = plt.figure(num=None, figsize=(26, 12), dpi=80, facecolor='w', edgecolor='k')
    t = ['2017-11-25 11:00:00', '2017-11-25 12:00:00', '2017-11-25 13:00:00', '2017-11-25 14:00:00',
         '2017-11-25 15:00:00', '2017-11-25 16:00:00', '2017-11-25 17:00:00', '2017-11-25 18:00:00']
    labels = ['c' + str(i) for i in range(10)]
    while True:
        plt.clf()
        for i in range(8):
            ax = plt.subplot(2, 4, i + 1)
            ax.set_title(t[i])
            cursor.execute(sql, (t[i],))
            result = cursor.fetchall()
            values = [ele['sales_volume'] for ele in result]
            ax.pie(values,
                   labels=labels,
                   shadow=True,  # 阴影
                   autopct='%1.1f%%',  # 添加百分比，浮点数，保留一位
                   startangle=180)

            ax.axis('equal')
        plt.show()
        plt.pause(5)
