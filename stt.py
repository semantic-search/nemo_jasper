from __future__ import absolute_import, division, print_function
import os
import wave
import argparse

import json
import nemo
import nemo.collections.asr as nemo_asr
from nemo.collections.asr.helpers import post_process_predictions


parser = argparse.ArgumentParser()
parser.add_argument(
    "--audio",type=str,
    required=True,
    help="Audio File Path"
)


class Stt:
    def __init__(self):
        self.neural_factory = nemo.core.NeuralModuleFactory(
            placement=nemo.core.DeviceType.GPU,
            cudnn_benchmark=True
        )

        self.asr_model = nemo_asr.models.ASRConvCTCModel.from_pretrained(
            model_info="JasperNet10x5-En-Base.nemo"
        )
        # Set this to True to enable beam search decoder
        self.ENABLE_NGRAM = True
        # This is only necessary if ENABLE_NGRAM = True. Otherwise, set to empty string
        self.LM_PATH = "WSJ_lm.binary"

        self.greedy_decoder = nemo_asr.GreedyCTCDecoder()
        self.labels = self.asr_model.vocabulary
        self.beam_search_with_lm = nemo_asr.BeamSearchDecoderWithLM(
            vocab=self.labels, beam_width=64, alpha=2.0, beta=1.5, lm_path=self.LM_PATH, num_cpus=max(os.cpu_count(), 1),
        )

    def wav_to_text(self, manifest, greedy=True):

        data_layer = nemo_asr.AudioToTextDataLayer(
            shuffle=False,
            manifest_filepath=manifest,
            labels=self.labels,
            batch_size=1
        )
        audio_signal, audio_signal_len, transcript, transcript_len = data_layer()
        log_probs, encoded_len = self.asr_model(input_signal=audio_signal, length=audio_signal_len)
        predictions = self.greedy_decoder(log_probs=log_probs)
        eval_tensors = [predictions]

        if self.ENABLE_NGRAM:
            print('Running with beam search')
            beam_predictions = self.beam_search_with_lm(
                log_probs=log_probs, log_probs_length=encoded_len)
            eval_tensors.append(beam_predictions)

        tensors = self.neural_factory.infer(tensors=eval_tensors)
        if greedy:
            prediction = post_process_predictions(tensors[0], self.labels)
        else:
            prediction = tensors[0][0][0][0][1]
        del data_layer
        del eval_tensors
        del beam_predictions
        del predictions
        del tensors
        del audio_signal, audio_signal_len, transcript, transcript_len
        del log_probs, encoded_len
        return prediction


    def create_manifest(self, duration, file_path):
        # create manifest
        manifest = dict()
        manifest['audio_filepath'] = file_path
        manifest['duration'] = duration
        manifest['text'] = 'todo'
        with open(file_path + ".json", 'w') as fout:
            fout.write(json.dumps(manifest))
        return file_path + ".json"


if __name__ == '__main__':
    args = parser.parse_args()
    file_name = args.audio
    fin = wave.open(file_name, 'rb')
    frames = fin.getnframes()
    rate = fin.getframerate()
    duration = frames / float(rate)
    duration = duration
    print(duration)
    fin.close()
    stt_obj = Stt()
    transcription = stt_obj.wav_to_text(stt_obj.create_manifest(duration, file_name))
    with open(file_name+".txt", "w") as text_file:
        text_file.write(transcription[0])
