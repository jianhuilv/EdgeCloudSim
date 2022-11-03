package edu.boun.edgecloudsim.applications.sample_app3;

import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.network.MM1Queue;

public class ExecuteDelayModel extends MM1Queue {

    private double PoissonMean; //seconds
    private double avgTaskLength; //MI

    public ExecuteDelayModel(int _numberOfMobileDevices, String _simScenario) {
        super(_numberOfMobileDevices, _simScenario);
    }

    @Override
    public void initialize() {

        PoissonMean=0;

        //Calculate interarrival time and task sizes
        double numOfTaskType = 0;
        SimSettings SS = SimSettings.getInstance();
        for (int i=0; i<SimSettings.getInstance().getTaskLookUpTable().length; i++) {
            double weight = SS.getTaskLookUpTable()[i][0]/(double)100;
            if(weight != 0) {
                PoissonMean += (SS.getTaskLookUpTable()[i][2])*weight;

                avgTaskLength += SS.getTaskLookUpTable()[i][7]*weight;

                numOfTaskType++;
            }
        }

        PoissonMean = PoissonMean/numOfTaskType;
        avgTaskLength = avgTaskLength/numOfTaskType;
    }


}
