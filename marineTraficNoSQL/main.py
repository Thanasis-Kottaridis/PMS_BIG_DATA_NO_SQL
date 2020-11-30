import dataPreprocessing
import mongoDBManager
import mortonCodeManager
import numpy as np

if __name__ == '__main__':

    ais_navigation_json = dataPreprocessing.preprocessAisDynamic()
    # mongoDBManager.insertData(ais_navigation_json)

    # EXTRACT A DUMMY MORTON CODE
    # TODO CREATE MORTON EXTENSION FOR CONVERTING
    # morton = mortonCodeManager.EncodeMorton4D(np.uint32(5), np.uint32(9), np.uint32(1), np.uint32(7))
    # print(morton)
    # print("X", mortonCodeManager.DecodeMorton4DX(morton))
    # print("Y", mortonCodeManager.DecodeMorton4DY(morton))
    # print("Z", mortonCodeManager.DecodeMorton4DZ(morton))
    # print("T", mortonCodeManager.DecodeMorton4Dt(morton))


