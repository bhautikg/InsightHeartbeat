import numpy as np
import datetime

def constantpeaks(bpm, fs, ecg =np.array([]), seconds = 6, fulldate=datetime.datetime.now()):
    length = fs*seconds
    fake_ecg = np.zeros((3, length))
    beatspersecond = 1/bpm * 60
    peaks = range(0,length,round(beatspersecond * fs))
    timestamps = []
    fake_ecg[:,peaks] = 1
    for i in range(length):
        fulldate = fulldate + datetime.timedelta(milliseconds=1000 / fs)
        timestamps.append(fulldate)
    timestamps = np.array(timestamps, dtype=str)
    fake_ecg = fake_ecg.astype(str)
    timestamps = timestamps[np.newaxis,:].transpose()
    print(timestamps.shape)
    print(fake_ecg.shape)
    fake_ecg = np.hstack([timestamps, fake_ecg.transpose()])
    if len(ecg) > 0:
        fake_ecg = np.vstack([ecg, fake_ecg])
    return fake_ecg, fulldate

if __name__ == '__main__':
    fulldate = datetime.datetime.now()
    fakeecg1, fulldate = constantpeaks(60, 360, seconds = 10, fulldate=fulldate)
    fakeecg2, fulldate = constantpeaks(72, 360, seconds = 10, ecg = fakeecg1, fulldate=fulldate)
    np.savetxt('../../data/fake003_signals.txt', fakeecg2, delimiter = ',', fmt='%s')
