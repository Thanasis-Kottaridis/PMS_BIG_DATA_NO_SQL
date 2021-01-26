from mongo.query import relationalQueries
from mongo.query import spatialQueries
from mongo.query import spatioTemporalQueries
from mongo.query import distanceJoinQueries
from mongo.query import trajectoryQueries

main_options = ['1', '2', '3', '4', '5', '0']


def main_menu():
    print("\n")
    print('|-------------- PMS MARINE TRAFFIC-----------------|')
    print('|                                                  |')
    print('| 1.  Relational queries                           |')
    print('| 2.  Spatial queries                              |')
    print('| 3.  Spatio-temporal queries                      |')
    print('| 4.  Distance Join Queries                        |')
    print('| 5.  Trajectory queries                           |')
    print('|                                                  |')
    print('| 0.  Exit                                         |')
    print('|--------------------------------------------------|')
    return input('Your choice: ')


if __name__ == '__main__' :
    choice = -1
    while choice not in main_options :
        choice = main_menu()

        # check if user choose relational queries
        if choice == '1' :
            print("--------------You choose Relational queries--------------")
            relationalQueries.executeRelationalQuery()
        elif choice == '2':
            print("--------------You choose Spatial queries--------------")
            spatialQueries.executeSpatialQuery()
        elif choice == '3' :
            print("--------------You choose Spatio-temporal queries--------------")
            spatioTemporalQueries.executeSpatioTemporalQuery()
        elif choice == '4' :
            print("--------------You choose Distance Join Queries--------------")
            distanceJoinQueries.executeDistanceJoinQuery()
        elif choice == '5' :
            print("--------------You choose Trajectory queries--------------")
            trajectoryQueries.executeTrajectoryQuery()



