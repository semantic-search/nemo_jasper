FROM pytorch/pytorch:1.5.1-cuda10.1-cudnn7-devel
RUN apt-get update && apt-get -y install swig
RUN apt-get install -y pkg-config libflac-dev libogg-dev libvorbis-dev libboost-dev
RUN apt-get install -y libsndfile1-dev python-setuptools libboost-all-dev python-dev
RUN apt-get install -y cmake
RUN apt-get install -y git
RUN apt-get install -y wget
RUN apt-get install -y unzip
RUN git clone https://github.com/NVIDIA/NeMo.git
RUN cd NeMo/scripts && ./install_decoders.sh
RUN wget --content-disposition https://api.ngc.nvidia.com/v2/models/nvidia/multidataset_jasper10x5dr/versions/5/zip -O multidataset_jasper10x5dr_5.zip
RUN wget  --content-disposition https://api.ngc.nvidia.com/v2/models/nvidia/wsj_lm_decoder/versions/1/zip -O wsj_lm_decoder_1.zip
RUN unzip multidataset_jasper10x5dr_5.zip
RUN unzip wsj_lm_decoder_1.zip
RUN git clone -q --depth 1 https://github.com/NVIDIA/apex && cd apex && pip install -q --no-cache-dir ./
RUN rm multidataset_jasper10x5dr_5.zip
RUN rm wsj_lm_decoder_1.zip
RUN rm Overview.md
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "main.py"]
