#include <QtTest>
#include <QTemporaryDir>
#include "../soundrecorder.h"

class SoundRecorderTest : public QObject {
    Q_OBJECT
private slots:
    void unavailableDevicesAreSafe() {
        QTemporaryDir dir;
        SoundRecorder recorder(QAudioDevice{}, QAudioDevice{}, dir.path());
        QSignalSpy done(&recorder, &SoundRecorder::soundRecordDone);
        QSignalSpy errors(&recorder, &SoundRecorder::audioError);
        recorder.stopRecord();
        recorder.stopPlay();
        QVERIFY(!recorder.startRecord());
        recorder.stopRecord();
        QVERIFY(!recorder.startPlay(QByteArray(320, '\0')));
        recorder.stopPlay();
        QCOMPARE(done.count(), 0);
        QCOMPARE(errors.count(), 2);
        QVERIFY(!QFile::exists(dir.filePath("tmpRecord.pcm")));
    }
    void emptyPlaybackIsRejected() {
        QTemporaryDir dir;
        SoundRecorder recorder(QAudioDevice{}, QAudioDevice{}, dir.path());
        QSignalSpy done(&recorder, &SoundRecorder::soundPlayDone);
        QSignalSpy errors(&recorder, &SoundRecorder::audioError);
        QVERIFY(!recorder.startPlay({}));
        recorder.stopPlay();
        QCOMPARE(done.count(), 0);
        QCOMPARE(errors.count(), 1);
    }
    void invalidStorageDoesNotEmitRecording() {
        QTemporaryDir dir;
        QFile blocker(dir.filePath("file"));
        QVERIFY(blocker.open(QIODevice::WriteOnly));
        blocker.close();
        SoundRecorder recorder(QAudioDevice{}, QAudioDevice{}, blocker.fileName() + "/sound");
        QSignalSpy done(&recorder, &SoundRecorder::soundRecordDone);
        QVERIFY(!recorder.startRecord());
        recorder.stopRecord();
        QVERIFY(!recorder.startPlay("pcm"));
        QCOMPARE(done.count(), 0);
    }
};
QTEST_GUILESS_MAIN(SoundRecorderTest)
#include "soundrecorder_test.moc"
