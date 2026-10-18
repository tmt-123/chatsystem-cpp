#include "soundrecorder.h"
#include <QDir>

/////////////////////////////////////////////
/// 单例模式
/////////////////////////////////////////////
SoundRecorder* SoundRecorder::instance = nullptr;

SoundRecorder *SoundRecorder::getInstance()
{
    if (instance == nullptr) {
        instance = new SoundRecorder();
    }
    return instance;
}

// 播放参考 https://www.cnblogs.com/tony-yang-flutter/p/16477212.html
// 录制参考 https://doc.qt.io/qt-6/qaudiosource.html
SoundRecorder::SoundRecorder(QObject *parent)
    : SoundRecorder(QMediaDevices::defaultAudioInput(), QMediaDevices::defaultAudioOutput(),
                    QStandardPaths::writableLocation(QStandardPaths::AppDataLocation) + "/sound", parent)
{}

SoundRecorder::SoundRecorder(const QAudioDevice& input, const QAudioDevice& output,
                             const QString& directory, QObject* parent)
    : QObject(parent)
{
    storageReady = QDir().mkpath(directory);
    soundFile.setFileName(directory + "/tmpRecord.pcm");
    inputFile.setFileName(directory + "/tmpPlay.pcm");
    // The speech service consumes 16 kHz mono signed 16-bit PCM. Do not send
    // bytes in a device's preferred format without converting them first.
    QAudioFormat format;
    format.setSampleRate(16000);
    format.setChannelCount(1);
    format.setSampleFormat(QAudioFormat::Int16);
    // Input failure must not prevent output initialization (and vice versa).
    if (!input.isNull() && input.isFormatSupported(format)) {
        audioSource = new QAudioSource(input, format, this);
        connect(audioSource, &QAudioSource::stateChanged, this, [this](QtAudio::State state) {
            if (state == QtAudio::StoppedState && recording && audioSource->error() != QtAudio::NoError) {
                recording = false;
                soundFile.close();
                emit recordingStopped();
                emit audioError("录音设备不可用，请检查麦克风后重试");
            }
        });
    }
    if (!output.isNull() && output.isFormatSupported(format)) {
        audioSink = new QAudioSink(output, format, this);
        connect(audioSink, &QAudioSink::stateChanged, this, [this](QtAudio::State state) {
            if (!playing) return;
            if (state == QtAudio::IdleState) {
                stopPlay();
            } else if (state == QtAudio::StoppedState && audioSink->error() != QtAudio::NoError) {
                stopPlay();
                emit audioError("播放设备不可用，请检查扬声器后重试");
            }
        });
    }
}

bool SoundRecorder::startRecord() {
    if (recording) return true;
    if (!audioSource) {
        emit audioError("没有可用的麦克风，或设备不支持 16 kHz 单声道 PCM 录音");
        return false;
    }
    if (!storageReady || !soundFile.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
        emit audioError("无法创建录音文件，请检查目录权限");
        return false;
    }
    recording = true;
    audioSource->start(&soundFile);
    if (audioSource->error() != QtAudio::NoError && recording) {
        recording = false;
        soundFile.close();
        emit recordingStopped();
        emit audioError("无法启动录音设备");
    }
    return recording;
}

void SoundRecorder::stopRecord() {
    if (!recording) return;
    recording = false; // stop() can emit stateChanged synchronously.
    audioSource->stop();
    const bool valid = soundFile.flush() && soundFile.error() == QFileDevice::NoError
            && soundFile.size() > 0 && audioSource->error() == QtAudio::NoError;
    soundFile.close();
    emit recordingStopped();
    if (valid) emit soundRecordDone(soundFile.fileName());
    else emit audioError("录音为空或写入失败，请重新录制");
}

bool SoundRecorder::startPlay(const QByteArray& content) {
    stopPlay();
    if (!audioSink || content.isEmpty()) {
        emit audioError(content.isEmpty() ? "语音内容为空，请稍后重试"
                         : "没有可用的扬声器，或设备不支持 16 kHz 单声道 PCM 播放");
        return false;
    }
    if (!storageReady || !inputFile.open(QIODevice::WriteOnly | QIODevice::Truncate)) {
        emit audioError("无法创建播放文件，请检查目录权限");
        return false;
    }
    const bool written = inputFile.write(content) == content.size() && inputFile.flush();
    inputFile.close();
    if (!written || !inputFile.open(QIODevice::ReadOnly)) {
        emit audioError("无法读取语音文件");
        return false;
    }
    playing = true;
    audioSink->start(&inputFile);
    if (audioSink->error() != QtAudio::NoError && playing) {
        stopPlay();
        emit audioError("无法启动播放设备");
    }
    return playing;
}

void SoundRecorder::stopPlay() {
    if (!playing) return;
    playing = false;
    if (audioSink) audioSink->stop();
    inputFile.close();
    emit soundPlayDone();
}
