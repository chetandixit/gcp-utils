from google.cloud import pubsub_v1


subscriber = pubsub_v1.SubscriberClient()
subscriber_path = subscriber.subscription_path('xw-winter-bloom-7', 'sub1')


def write_msg(msg):
    msglog = open('msg.log', 'a+')
    msglog.write(msg + "\n")
    msglog.close()


def callback(message):
    write_msg(message.data)
#    print(message.data)
    message.ack()


future = subscriber.subscribe(subscriber_path, callback)

future.result()
