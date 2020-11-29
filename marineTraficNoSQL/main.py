import dataPreprocessing
import mongoDBManager

if __name__ == '__main__':

    ais_navigation_json = dataPreprocessing.preprocessAisDynamic()
    mongoDBManager.insertData(ais_navigation_json)

