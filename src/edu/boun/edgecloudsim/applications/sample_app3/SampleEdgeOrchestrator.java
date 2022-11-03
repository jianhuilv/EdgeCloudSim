/*
 * Title:        EdgeCloudSim - Edge Orchestrator
 * 
 * Description: 
 * SampleEdgeOrchestrator offloads tasks to proper server
 * In this scenario mobile devices can also execute tasks
 * 
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 * Copyright (c) 2017, Bogazici University, Istanbul, Turkey
 */

package edu.boun.edgecloudsim.applications.sample_app3;

import java.util.List;

import edu.boun.edgecloudsim.edge_server.EdgeHost;
import edu.boun.edgecloudsim.network.NetworkModel;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;

import edu.boun.edgecloudsim.core.SimManager;
import edu.boun.edgecloudsim.core.SimSettings;
import edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator;
import edu.boun.edgecloudsim.edge_server.EdgeVM;
import edu.boun.edgecloudsim.edge_client.CpuUtilizationModel_Custom;
import edu.boun.edgecloudsim.edge_client.Task;
import edu.boun.edgecloudsim.edge_client.mobile_processing_unit.MobileVM;
import edu.boun.edgecloudsim.utils.SimLogger;

public class SampleEdgeOrchestrator extends EdgeOrchestrator {
	
	private int numberOfHost; //used by load balancer

	public SampleEdgeOrchestrator(String _policy, String _simScenario) {
		super(_policy, _simScenario);
	}

	@Override
	public void initialize() {
		numberOfHost=SimSettings.getInstance().getNumOfEdgeHosts();
	}

	/*
	 * (non-Javadoc)
	 * @see edu.boun.edgecloudsim.edge_orchestrator.EdgeOrchestrator#getDeviceToOffload(edu.boun.edgecloudsim.edge_client.Task)
	 * 
	 */
	@Override
	public int getDeviceToOffload(Task task) {
		int result = 0;

		if(policy.equals("IMPROVED_MCT")){

//			List<MobileVM> MobileVMArray = SimManager.getInstance().getMobileServerManager().getVmList(task.getAssociatedHostId());


			MobileVM SelectedMDVM = null;
			EdgeVM SelectedEdgeVM = null;
			int maxHostID = 0;
			int maxVmID = 0;
			double maxTarU = Integer.MIN_VALUE;
			// 分别计算
			// 1. 本地移动设备的T


			// 2. 单纯Edge的T
			// 2.1 找到最有空闲的机器
			double curTarUtilization=0;
			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);

				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					curTarUtilization = vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(curTarUtilization>maxTarU){
						maxTarU = 100-curTarUtilization;
						maxHostID = hostIndex;
						maxVmID = vmIndex;
						SelectedEdgeVM = vmArray.get(vmIndex);
					}
				}
			}
			// 2.2 计算T



			// 3. 计算所有空闲deviceServer的T
			// 3.1 计算最空闲的设备

			for(int hostIndex=0; hostIndex<SimManager.getInstance().getNumOfMobileDevice(); hostIndex++){
				List<MobileVM> vmArray = SimManager.getInstance().getMobileServerManager().getVmList(hostIndex);
				 for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if(requiredCapacity <= targetVmCapacity){
						SelectedMDVM = vmArray.get(vmIndex);
						break;
					}
				}
			}
			// 3.1 计算指定deviceServer的T


			// 选用T最小的方式
			result = SimSettings.GENERIC_EDGE_DEVICE_ID;


		}
		else {
			SimLogger.printLine("Unknow edge orchestrator policy! Terminating simulation...");
			System.exit(0);
		}

		return result;
	}

//	@Override
//	public Vm getVmToOffload(Task task, int deviceId) {
//		Vm selectedVM = null;
//
//		if (deviceId == SimSettings.MOBILE_DATACENTER_ID) {
//			List<MobileVM> vmArray = SimManager.getInstance().getMobileServerManager().getVmList(task.getMobileDeviceId());
//			double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(0).getVmType());
//			double targetVmCapacity = (double) 100 - vmArray.get(0).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
//
//			if (requiredCapacity <= targetVmCapacity)
//				selectedVM = vmArray.get(0);
//		 }
//		else if(deviceId == SimSettings.GENERIC_EDGE_DEVICE_ID){
//			//Select VM on edge devices via Least Loaded algorithm!
//			double selectedVmCapacity = 0; //start with min value
//			for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
//				List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);
//				for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
//					double requiredCapacity = ((CpuUtilizationModel_Custom)task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
//					double targetVmCapacity = (double)100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
//					if(requiredCapacity <= targetVmCapacity && targetVmCapacity > selectedVmCapacity){
//						selectedVM = vmArray.get(vmIndex);
//						selectedVmCapacity = targetVmCapacity;
//					}
//				}
//			}
//		}
//		else{
//			SimLogger.printLine("Unknown device id! The simulation has been terminated.");
//			System.exit(0);
//		}
//
//		return selectedVM;
//	}
	@Override
	public Vm getVmToOffload(Task task, int deviceId) {

		// 选中的VM
		Vm selectedVM = null;
		MobileVM selectedMDVM = null;
		EdgeVM selectedEdgeVM = null;

		// 三种方式各自的总耗时
		double TLocal = 0;
		double TEdge = 0;
		double TMobileServer = 0;
		double TMin = Integer.MAX_VALUE;
		// 网络延迟模型
		NetworkModel networkModel = SimManager.getInstance().getNetworkModel();

		// 泊松系数,代表任务每秒产生多少个任务
		double taskArrivalRate = 1/SimSettings.getInstance().getTaskLookUpTable()[task.getTaskType()][2];
		// 分三种类型，计算各自的一个T total， 然后取最好的一个vm
		// 分别计算
		// 1. 本地移动设备的T T = 当前设备的执行时间+等待时间
		List<MobileVM> curMobileVMList = SimManager.getInstance().getMobileServerManager().getVmList(task.getMobileDeviceId());
		MobileVM curMobileVM = curMobileVMList.get(0);
		if(curMobileVMList!=null&& curMobileVMList.size()!=0){
			double taskLength = task.getCloudletLength();
			double taskServingRate = curMobileVM.getMips()/taskLength;
			TLocal = 1/(taskServingRate-taskArrivalRate) + 1/taskServingRate;
			TMin = Math.min(TLocal, TMin);
		}
		// 2. 单纯Edge的T
		// 2.1 找到最有空闲的机器
		double maxTarU = Integer.MIN_VALUE;
		double curTarUtilization=0;
		for(int hostIndex=0; hostIndex<numberOfHost; hostIndex++){
			List<EdgeVM> vmArray = SimManager.getInstance().getEdgeServerManager().getVmList(hostIndex);

			for(int vmIndex=0; vmIndex<vmArray.size(); vmIndex++){
				curTarUtilization = vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
				if(curTarUtilization>maxTarU){
					maxTarU = 100-curTarUtilization;
					selectedEdgeVM = vmArray.get(vmIndex);
				}
			}
		}

		// 2.2 计算SelectedEdgeVM的T total,T = 在边缘服务器的等待时长+边缘服务器执行时长+设备到服务器的传输延迟
		// 当不足以服务的时候，会返回 delay = 0
		double networkDelay = networkModel.getUploadDelay(task.getMobileDeviceId(), SimSettings.GENERIC_EDGE_DEVICE_ID, task);
		if(selectedEdgeVM!=null &&networkDelay>0){
			double taskLength = task.getCloudletLength();
			double taskServingRate = selectedEdgeVM.getMips()/taskLength;
			TEdge = (1/(taskServingRate-taskArrivalRate) + 1/taskServingRate)+ networkDelay;
			TMin = Math.min(TEdge, TMin);
		}
		// 3. 计算所有空闲deviceServer的T
		// 3.1 计算最空闲的设备
		if(policy.equals("EESC")) {
			for(int hostIndex = 0; hostIndex < SimManager.getInstance().getNumOfMobileDevice(); hostIndex++) {
				if (hostIndex == task.getMobileDeviceId()){
					continue;
				}
				List<MobileVM> vmArray = SimManager.getInstance().getMobileServerManager().getVmList(hostIndex);
				for (int vmIndex = 0; vmIndex < vmArray.size(); vmIndex++) {
					double requiredCapacity = ((CpuUtilizationModel_Custom) task.getUtilizationModelCpu()).predictUtilization(vmArray.get(vmIndex).getVmType());
					double targetVmCapacity = (double) 100 - vmArray.get(vmIndex).getCloudletScheduler().getTotalUtilizationOfCpu(CloudSim.clock());
					if (requiredCapacity <= targetVmCapacity) {
						selectedMDVM = vmArray.get(vmIndex);
						break;
					}
				}
			}
		}
		// 3.1 计算指定SelectedMDVM的T，T = 当前设备到边缘服务器的传输延迟+边缘服务器到被指定服务器的传输延迟+被指定的设备队列等待时间+执行时间

		if(selectedMDVM!=null &&networkDelay>0){
			double networkDelayOfEdgeToMobileService = networkModel.getUploadDelay(selectedMDVM.getId(), SimSettings.GENERIC_EDGE_DEVICE_ID, task)+networkDelay;
			double taskLength = task.getCloudletLength();
			double taskServingRate = selectedMDVM.getMips()/taskLength;
			TMobileServer = (1/(taskServingRate-taskArrivalRate) + 1/taskServingRate)+ networkDelayOfEdgeToMobileService;
			TMin = Math.min(TMobileServer, TMin);
		}


		// 比较三个结果，选用T最小的方式
		if(TMin ==TLocal&& curMobileVM!=null){
			selectedVM = curMobileVM;
		}
		else if (TMin == TEdge&&selectedEdgeVM!=null){
			selectedVM = selectedEdgeVM;
		}
		else if (TMin==TMobileServer&&selectedMDVM!=null){
			selectedVM = selectedMDVM;

		}

		return selectedVM;
	}

	@Override
	public void processEvent(SimEvent arg0) {
		// Nothing to do!
	}

	@Override
	public void shutdownEntity() {
		// Nothing to do!
	}

	@Override
	public void startEntity() {
		// Nothing to do!
	}

}