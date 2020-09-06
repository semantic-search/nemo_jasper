import subprocess
import os
import json
from quartznet.db_models.mongo_setup import global_init
from quartznet.db_models.models import Cache
from quartznet import globals, init


def send_to_topic(topic, value_to_send_dic):
    data_json = json.dumps(value_to_send_dic)
    init.producer_obj.send(topic, value=data_json)


if __name__ == '__main__':
    for message in init.consumer_obj:
        global_init()
        message = message.value
        db_key = str(message)
        db_object = Cache.objects.get(pk=db_key)
        file_name = db_object.file_name
        init.redis_obj.set(globals.RECEIVE_TOPIC, file_name)
        if db_object.is_doc_type:
            pass
        else:
            if db_object.mime_type == "wav":
                with open(file_name, 'wb') as file_to_save:
                    file_to_save.write(db_object.file.read())
                subprocess.call(["python", "stt.py", "--audio", file_name])
                with open(file_name+".txt") as tran:
                    transcription = tran.read()
                os.remove(file_name)
                os.remove(file_name+".json")
                os.remove(file_name+".txt")
                final_full_response = {
                    "container_name": globals.RECEIVE_TOPIC,
                    "file_name": file_name,
                    "results": transcription,
                    "is_doc_type": False
                }
                send_to_topic(globals.SEND_TOPIC_TEXT, value_to_send_dic=final_full_response)
                init.producer_obj.flush()
