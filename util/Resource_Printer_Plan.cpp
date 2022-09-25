//
// Created by ruihong on 9/24/22.
//

#include "Resource_Printer_Plan.h"

int cpu_id_arr[NUMA_CORE_NUM] = {84,85,86,87,88,89,90,91,92,93,94,95,180,181,182,183,184,185,186,187,188,189,190,191};

Resource_Printer_PlanA::Resource_Printer_PlanA() {


    std::string prefix = "cpu";
    std::string str;
    std::string suffix = " %llu %llu %llu %llu";
    for (int i = 0; i < NUMA_CORE_NUM; ++i) {
      FILE* file = fopen("/proc/stat", "r");
      std::string s = std::to_string(cpu_id_arr[i]);
      str = prefix + s +suffix;
      int ret = fscanf(file, str.c_str(), &lastTotalUser[i], &lastTotalUserLow[i],
                       &lastTotalSys[i], &lastTotalIdle[i]);
      assert(ret != 0);
      fclose(file);
    }


}
long double Resource_Printer_PlanA::getCurrentValue() { long double percent[NUMA_CORE_NUM] = {};
  long double aggre_percent = 0;
  FILE* file;
  unsigned long long totalUser[NUMA_CORE_NUM], totalUserLow[NUMA_CORE_NUM], totalSys[NUMA_CORE_NUM], totalIdle[NUMA_CORE_NUM], total[NUMA_CORE_NUM];

  std::string prefix = "cpu";
  std::string str;
  std::string suffix = " %llu %llu %llu %llu";




  for (int i = 0; i < NUMA_CORE_NUM; ++i) {
    file = fopen("/proc/stat", "r");
    fscanf(file, "cpu %llu %llu %llu %llu", &totalUser, &totalUserLow,
           &totalSys, &totalIdle);
    if (totalUser[i] < lastTotalUser[i] || totalUserLow[i] < lastTotalUserLow[i] ||
        totalSys[i] < lastTotalSys[i] || totalIdle[i] < lastTotalIdle[i]){
      //Overflow detection. Just skip this value.
      percent[i] = -1.0;
      fclose(file);
      break ;
    }
    else{
      total[i] = (totalUser[i] - lastTotalUser[i]) + (totalUserLow[i] - lastTotalUserLow[i]) +
                 (totalSys[i] - lastTotalSys[i]);
      percent[i] = total[i];
      total[i] += (totalIdle[i] - lastTotalIdle[i]);
      percent[i] /= total[i];
      percent[i] *= 100;
    }

    lastTotalUser[i] = totalUser[i];
    lastTotalUserLow[i] = totalUserLow[i];
    lastTotalSys[i] = totalSys[i];
    lastTotalIdle[i] = totalIdle[i];
    fclose(file);
  }
  for (int i = 0; i < NUMA_CORE_NUM; ++i) {
    aggre_percent += percent[i];
    if (percent[i] == -1.0){
      return -1.0;
    }
  }
  aggre_percent = aggre_percent/NUMA_CORE_NUM;
  return aggre_percent;
}


Resource_Printer_PlanB::Resource_Printer_PlanB() {





}
long double Resource_Printer_PlanB::getCurrentValue() {
  struct tms timeSample;
  clock_t now;
  long double percent;

  now = times(&timeSample);
  if (now <= lastCPU || timeSample.tms_stime < lastSysCPU ||
      timeSample.tms_utime < lastUserCPU){
    //Overflow detection. Just skip this value.
    percent = -1.0;
  }
  else{
    percent = (timeSample.tms_stime - lastSysCPU) +
              (timeSample.tms_utime - lastUserCPU);
    percent /= (now - lastCPU);
    percent /= NUMA_CORE_NUM;
    percent *= 100;
  }
  lastCPU = now;
  lastSysCPU = timeSample.tms_stime;
  lastUserCPU = timeSample.tms_utime;

  return percent;

}