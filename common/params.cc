#include "common/params.h"

#include <dirent.h>
#include <sys/file.h>

#include <algorithm>
#include <cassert>
#include <csignal>
#include <unordered_map>

#include "common/queue.h"
#include "common/swaglog.h"
#include "common/util.h"
#include "system/hardware/hw.h"

namespace {

volatile sig_atomic_t params_do_exit = 0;
void params_sig_handler(int signal) {
  params_do_exit = 1;
}

int fsync_dir(const std::string &path) {
  int result = -1;
  int fd = HANDLE_EINTR(open(path.c_str(), O_RDONLY, 0755));
  if (fd >= 0) {
    result = fsync(fd);
    close(fd);
  }
  return result;
}

bool create_params_path(const std::string &param_path, const std::string &key_path) {
  // Make sure params path exists
  if (!util::file_exists(param_path) && !util::create_directories(param_path, 0775)) {
    return false;
  }

  // See if the symlink exists, otherwise create it
  if (!util::file_exists(key_path)) {
    // 1) Create temp folder
    // 2) Symlink it to temp link
    // 3) Move symlink to <params>/d

    std::string tmp_path = param_path + "/.tmp_XXXXXX";
    // this should be OK since mkdtemp just replaces characters in place
    char *tmp_dir = mkdtemp((char *)tmp_path.c_str());
    if (tmp_dir == NULL) {
      return false;
    }

    std::string link_path = std::string(tmp_dir) + ".link";
    if (symlink(tmp_dir, link_path.c_str()) != 0) {
      return false;
    }

    // don't return false if it has been created by other
    if (rename(link_path.c_str(), key_path.c_str()) != 0 && errno != EEXIST) {
      return false;
    }
  }

  return true;
}

std::string ensure_params_path(const std::string &prefix, const std::string &path = {}) {
  std::string params_path = path.empty() ? Path::params() : path;
  if (!create_params_path(params_path, params_path + prefix)) {
    throw std::runtime_error(util::string_format(
        "Failed to ensure params path, errno=%d, path=%s, param_prefix=%s",
        errno, params_path.c_str(), prefix.c_str()));
  }
  return params_path;
}

class FileLock {
public:
  FileLock(const std::string &fn) {
    fd_ = HANDLE_EINTR(open(fn.c_str(), O_CREAT, 0775));
    if (fd_ < 0 || HANDLE_EINTR(flock(fd_, LOCK_EX)) < 0) {
      LOGE("Failed to lock file %s, errno=%d", fn.c_str(), errno);
    }
  }
  ~FileLock() { close(fd_); }

private:
  int fd_ = -1;
};

std::unordered_map<std::string, uint32_t> keys = {
    {"访问令牌", CLEAR_ON_MANAGER_START | DONT_LOG},
    {"Api缓存_设备", PERSISTENT},
    {"Api缓存_导航目的地", PERSISTENT},
    {"AssistNow令牌", PERSISTENT},
    {"Athenad进程ID", PERSISTENT},
    {"Athenad上传队列", PERSISTENT},
    {"Athenad最近查看的路线", PERSISTENT},
    {"启动次数", PERSISTENT},
    {"校准参数", PERSISTENT},
    {"相机调试曝光增益", CLEAR_ON_MANAGER_START},
    {"相机调试曝光时间", CLEAR_ON_MANAGER_START},
    {"汽车电池容量", PERSISTENT},
    {"汽车参数", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"汽车参数缓存", CLEAR_ON_MANAGER_START},
    {"汽车参数持久", PERSISTENT},
    {"汽车参数上一路线", PERSISTENT},
    {"汽车Vin", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"完成训练版本", PERSISTENT},
    {"控制就绪", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"当前启动日志", PERSISTENT},
    {"当前路线", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"禁用日志", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"禁用关机", PERSISTENT},
    {"禁用更新", PERSISTENT},
    {"离开时加速器", PERSISTENT},
    {"Dm模型初始化", CLEAR_ON_ONROAD_TRANSITION},
    {"短信中心ID", PERSISTENT},
    {"执行重启", CLEAR_ON_MANAGER_START},
    {"执行关机", CLEAR_ON_MANAGER_START},
    {"执行卸载", CLEAR_ON_MANAGER_START},
    {"实验性纵向控制启用", PERSISTENT | DEVELOPMENT_ONLY},
    {"实验模式", PERSISTENT},
    {"实验模式已确认", PERSISTENT},
    {"固件查询完成", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"强制关机", PERSISTENT},
    {"Git分支", PERSISTENT},
    {"Git提交", PERSISTENT},
    {"Git提交日期", PERSISTENT},
    {"Git差异", PERSISTENT},
    {"GithubSsh密钥", PERSISTENT},
    {"Github用户名", PERSISTENT},
    {"Git远程", PERSISTENT},
    {"GsmApn", PERSISTENT},
    {"Gsm计量", PERSISTENT},
    {"Gsm漫游", PERSISTENT},
    {"硬件序列号", PERSISTENT},
    {"已接受条款", PERSISTENT},
    {"IMEI", PERSISTENT},
    {"安装日期", PERSISTENT},
    {"驾驶员视图启用", CLEAR_ON_MANAGER_START},
    {"已启动", PERSISTENT},
    {"Ldw启用", PERSISTENT},
    {"使用公制", PERSISTENT},
    {"离线", CLEAR_ON_MANAGER_START},
    {"在线", PERSISTENT},
    {"检测到右驾驶", PERSISTENT},
    {"是发布分支", CLEAR_ON_MANAGER_START},
    {"正在拍摄快照", CLEAR_ON_MANAGER_START},
    {"是测试分支", CLEAR_ON_MANAGER_START},
    {"操纵杆调试模式", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"语言设置", PERSISTENT},
    {"最后AthenaPing时间", CLEAR_ON_MANAGER_START},
    {"最后GPS位置", PERSISTENT},
    {"最后Manager退出原因", CLEAR_ON_MANAGER_START},
    {"最后离线状态包", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"最后电源掉落检测", CLEAR_ON_MANAGER_START},
    {"最后更新异常", CLEAR_ON_MANAGER_START},
    {"最后更新时间", PERSISTENT},
    {"实时参数", PERSISTENT},
    {"实时扭矩参数", PERSISTENT | DONT_LOG},
    {"纵向个性", PERSISTENT},
    {"导航目的地", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"导航目的地路标", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"导航过去的目的地", PERSISTENT},
    {"导航设置左侧", PERSISTENT},
    {"导航设置24小时制", PERSISTENT},
    {"网络计量", PERSISTENT},
    {"Obd多路复用改变", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"Obd多路复用启用", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"离线_BadNvme", CLEAR_ON_MANAGER_START},
    {"离线_车辆无法识别", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"离线_需要连接", CLEAR_ON_MANAGER_START},
    {"离线_需要连接提示", CLEAR_ON_MANAGER_START},
    {"离线_无效时间", CLEAR_ON_MANAGER_START},
    {"离线_正在拍摄快照", CLEAR_ON_MANAGER_START},
    {"离线_Neos更新", CLEAR_ON_MANAGER_START},
    {"离线_无固件", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"离线_重新校准", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"离线_存储丢失", CLEAR_ON_MANAGER_START},
    {"离线_温度过高", CLEAR_ON_MANAGER_START},
    {"离线_非官方硬件", CLEAR_ON_MANAGER_START},
    {"离线_更新失败", CLEAR_ON_MANAGER_START},
    {"Openpilot启用切换", PERSISTENT},
    {"Panda心跳丢失", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"PandaSom重置触发", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"Panda签名", CLEAR_ON_MANAGER_START},
    {"Prime类型", PERSISTENT},
    {"记录前方", PERSISTENT},
    {"记录前方锁", PERSISTENT},  // for the internal fleet
    {"重播控制状态", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"路线计数", PERSISTENT},
    {"推迟更新", CLEAR_ON_MANAGER_START | CLEAR_ON_OFFROAD_TRANSITION},
    {"Ssh启用", PERSISTENT},
    {"条款版本", PERSISTENT},
    {"时区", PERSISTENT},
    {"训练版本", PERSISTENT},
    {"Ublox可用", PERSISTENT},
    {"更新可用", CLEAR_ON_MANAGER_START | CLEAR_ON_ONROAD_TRANSITION},
    {"更新失败计数", CLEAR_ON_MANAGER_START},
    {"更新器可用分支", PERSISTENT},
    {"更新器当前描述", CLEAR_ON_MANAGER_START},
    {"更新器当前发布说明", CLEAR_ON_MANAGER_START},
    {"更新器获取可用", CLEAR_ON_MANAGER_START},
    {"更新器新描述", CLEAR_ON_MANAGER_START},
    {"更新器新发布说明", CLEAR_ON_MANAGER_START},
    {"更新器状态", CLEAR_ON_MANAGER_START},
    {"更新器目标分支", CLEAR_ON_MANAGER_START},
    {"更新器最后获取时间", PERSISTENT},
    {"版本", PERSISTENT},
    {"视觉雷达切换", PERSISTENT},
    {"轮式车体", PERSISTENT},
    {"选择的车", PERSISTENT},
    {"支持的车", PERSISTENT},
    {"支持的车_gm", PERSISTENT},
    { "显示调试UI", PERSISTENT },
    { "显示日期时间", PERSISTENT },
    { "显示Hud模式", PERSISTENT },
    { "显示转向旋转", PERSISTENT },
    { "显示路径结束", PERSISTENT },
    { "显示加速Rpm", PERSISTENT },
    { "显示Tpms", PERSISTENT },
    { "显示转向模式", PERSISTENT },
    { "显示设备状态", PERSISTENT },
    { "显示连接信息", PERSISTENT },
    { "显示车道信息", PERSISTENT },
    { "显示盲点", PERSISTENT },
    { "显示间隙信息", PERSISTENT },
    { "显示Dm信息", PERSISTENT },
    { "显示雷达信息", PERSISTENT },
    { "混合雷达信息", PERSISTENT },
    { "胡萝卜测试", PERSISTENT },
    { "显示路径模式", PERSISTENT },
    { "显示路径颜色", PERSISTENT },
    { "显示路径模式巡航关闭", PERSISTENT },
    { "显示路径颜色巡航关闭", PERSISTENT },
    { "显示路径模式车道", PERSISTENT },
    { "显示路径颜色车道", PERSISTENT },
    { "显示路径宽度", PERSISTENT },
    { "显示绘图模式", PERSISTENT },
    { "自动恢复从气体速度", PERSISTENT },
    { "自动取消从气体模式", PERSISTENT },
    { "自动巡航控制", PERSISTENT },
    { "Mapbox样式", PERSISTENT },
    { "自动曲线速度下限", PERSISTENT },
    { "自动曲线速度因子", PERSISTENT },
    { "自动曲线速度侵略性", PERSISTENT },
    { "自动转向控制", PERSISTENT },
    { "自动车道更改速度", PERSISTENT },
    { "自动转向控制速度车道更改", PERSISTENT },
    { "自动转向控制速度转向", PERSISTENT },
    { "自动转向控制转向结束", PERSISTENT },
    { "自动转向地图更改", PERSISTENT }, 
    { "车道更改需要扭矩", PERSISTENT }, 
    { "自动导航速度控制", PERSISTENT },
    { "自动导航速度控制结束", PERSISTENT },
    { "自动导航速度减速时间", PERSISTENT },
    { "自动导航速度减速速度", PERSISTENT },
    { "自动导航速度减速率", PERSISTENT },
    { "自动导航速度安全因子", PERSISTENT },
    { "自动恢复从刹车释放交通标志", PERSISTENT },
    { "停止加速应用", PERSISTENT },
    { "停止加速", PERSISTENT },
    { "开始加速应用", PERSISTENT },
    { "自动速度达到道路限速", PERSISTENT },
    { "应用长动态成本", PERSISTENT },
    { "停止距离胡萝卜", PERSISTENT },
    { "ALeadTau", PERSISTENT },
    { "ALeadTau开始", PERSISTENT },
    { "巡航按钮模式", PERSISTENT },
    { "巡航按钮测试1", PERSISTENT },
    { "巡航按钮测试2", PERSISTENT },
    { "巡航按钮测试3", PERSISTENT },
    { "巡航速度单位", PERSISTENT },
    { "巡航速度最小", PERSISTENT },
    { "我的驾驶模式", PERSISTENT },
    { "我的安全模式因子", PERSISTENT },
    { "巡航Eco控制", PERSISTENT },
    { "转向执行器延迟", PERSISTENT },
    { "巡航开启距离", PERSISTENT },
    { "我的Eco模式因子", PERSISTENT },
    { "巡航最大值1", PERSISTENT },
    { "巡航最大值2", PERSISTENT },
    { "巡航最大值3", PERSISTENT },
    { "巡航最大值4", PERSISTENT },
    { "巡航最大值5", PERSISTENT },
    { "巡航最大值6", PERSISTENT },
    { "巡航最小值", PERSISTENT },
    { "纵向调整KpV", PERSISTENT },
    { "纵向调整KiV", PERSISTENT },
    { "纵向调整Kf", PERSISTENT },
    { "启用雷达轨迹", PERSISTENT },
    { "启用AVM", PERSISTENT },
    { "启动时热点", PERSISTENT },
    { "Scc连接Bus2", PERSISTENT },
    { "CanfdHDA2", PERSISTENT },
    { "声音音量调整", PERSISTENT },
    { "声音音量调整接合", PERSISTENT },
    { "开始记录", PERSISTENT },
    { "停止记录", PERSISTENT },
    { "TFollow速度增加", PERSISTENT },
    { "TFollow速度增加M", PERSISTENT },
    { "TFollow间隙1", PERSISTENT },
    { "TFollow间隙2", PERSISTENT },
    { "TFollow间隙3", PERSISTENT },
    { "TFollow间隙4", PERSISTENT },
    { "当速度相机时触觉反馈", PERSISTENT },
    { "使用车道线速度", PERSISTENT },
    { "使用车道线速度应用", PERSISTENT },
    { "调整车道偏移", PERSISTENT },
    { "调整曲线偏移", PERSISTENT },
    { "路径偏移", PERSISTENT },
    { "最大角度帧", PERSISTENT },
    { "软保持模式", PERSISTENT },
    { "横向扭矩自定义", PERSISTENT },
    { "横向扭矩加速因子", PERSISTENT },
    { "横向扭矩摩擦", PERSISTENT },
    { "自定义转向最大", PERSISTENT },
    { "自定义转向增量上", PERSISTENT },
    { "自定义转向增量下", PERSISTENT },
    { "速度来自PCM", PERSISTENT },
    { "胡萝卜记录", PERSISTENT },
    { "胡萝卜显示", PERSISTENT },
    { "MSLC启用", PERSISTENT },
    { "无日志", PERSISTENT },
    { "胡萝卜异常", CLEAR_ON_MANAGER_START },
    { "胡萝卜路线活动", CLEAR_ON_MANAGER_START },
    { "车名", CLEAR_ON_MANAGER_START },
    { "横向路径成本", PERSISTENT },
    { "横向运动成本", PERSISTENT },
    { "横向加速成本", PERSISTENT },
    { "横向转向速率成本", PERSISTENT },

    { "始终开启横向控制", PERSISTENT},
    { "GMap密钥", PERSISTENT},
    {"AMap密钥1", PERSISTENT},
    {"AMap密钥2", PERSISTENT},
    { "Mapbox公钥", PERSISTENT},
    { "Mapbox私钥", PERSISTENT},
    { "搜索输入", PERSISTENT},
    { "转向比率", PERSISTENT },
    { "静音门", PERSISTENT },
    { "静音安全带", PERSISTENT },
    { "长俯仰", PERSISTENT },
    { "EV表", PERSISTENT },
    { "气体再生命令", PERSISTENT },
    { "锁门", PERSISTENT },
    { "SNGHack", PERSISTENT },
    { "TSS2调优", PERSISTENT },
    {"最后地图更新", PERSISTENT},
    {"地图选择", PERSISTENT},
    {"地图曲率", PERSISTENT},
    {"地图速度限制", PERSISTENT},
    {"下一地图速度限制", PERSISTENT},
    {"地图目标LatA", PERSISTENT},
    {"地图目标速度", PERSISTENT},
    {"MTSC侵略性", PERSISTENT},
    {"MTSC曲率检查", PERSISTENT},
    {"MTSC启用", PERSISTENT},
    {"NNFF", PERSISTENT},
    {"NNFF模型模糊匹配", PERSISTENT},
    {"NNFF模型名称", PERSISTENT},
    {"OSM下载位置", PERSISTENT},
    {"OSM下载进度", CLEAR_ON_MANAGER_START},
    {"首选时间表", PERSISTENT},
    {"道路名称", PERSISTENT},
    {"道路名称UI", PERSISTENT},
    {"时间表待处理", PERSISTENT},
    {"使用横向急动", PERSISTENT},
};

} // namespace


Params::Params(const std::string &path) {
  params_prefix = "/" + util::getenv("OPENPILOT_PREFIX", "d");
  params_path = ensure_params_path(params_prefix, path);
}

Params::~Params() {
  if (future.valid()) {
    future.wait();
  }
  assert(queue.empty());
}

std::vector<std::string> Params::allKeys() const {
  std::vector<std::string> ret;
  for (auto &p : keys) {
    ret.push_back(p.first);
  }
  return ret;
}

bool Params::checkKey(const std::string &key) {
  return keys.find(key) != keys.end();
}

ParamKeyType Params::getKeyType(const std::string &key) {
  return static_cast<ParamKeyType>(keys[key]);
}

int Params::put(const char* key, const char* value, size_t value_size) {
  // Information about safely and atomically writing a file: https://lwn.net/Articles/457667/
  // 1) Create temp file
  // 2) Write data to temp file
  // 3) fsync() the temp file
  // 4) rename the temp file to the real name
  // 5) fsync() the containing directory
  std::string tmp_path = params_path + "/.tmp_value_XXXXXX";
  int tmp_fd = mkstemp((char*)tmp_path.c_str());
  if (tmp_fd < 0) return -1;

  int result = -1;
  do {
    // Write value to temp.
    ssize_t bytes_written = HANDLE_EINTR(write(tmp_fd, value, value_size));
    if (bytes_written < 0 || (size_t)bytes_written != value_size) {
      result = -20;
      break;
    }

    // fsync to force persist the changes.
    if ((result = fsync(tmp_fd)) < 0) break;

    FileLock file_lock(params_path + "/.lock");

    // Move temp into place.
    if ((result = rename(tmp_path.c_str(), getParamPath(key).c_str())) < 0) break;

    // fsync parent directory
    result = fsync_dir(getParamPath());
  } while (false);

  close(tmp_fd);
  ::unlink(tmp_path.c_str());
  return result;
}

int Params::remove(const std::string &key) {
  FileLock file_lock(params_path + "/.lock");
  int result = unlink(getParamPath(key).c_str());
  if (result != 0) {
    return result;
  }
  return fsync_dir(getParamPath());
}

std::string Params::get(const std::string &key, bool block) {
  if (!block) {
    return util::read_file(getParamPath(key));
  } else {
    // blocking read until successful
    params_do_exit = 0;
    void (*prev_handler_sigint)(int) = std::signal(SIGINT, params_sig_handler);
    void (*prev_handler_sigterm)(int) = std::signal(SIGTERM, params_sig_handler);

    std::string value;
    while (!params_do_exit) {
      if (value = util::read_file(getParamPath(key)); !value.empty()) {
        break;
      }
      util::sleep_for(100);  // 0.1 s
    }

    std::signal(SIGINT, prev_handler_sigint);
    std::signal(SIGTERM, prev_handler_sigterm);
    return value;
  }
}

std::map<std::string, std::string> Params::readAll() {
  FileLock file_lock(params_path + "/.lock");
  return util::read_files_in_dir(getParamPath());
}

void Params::clearAll(ParamKeyType key_type) {
  FileLock file_lock(params_path + "/.lock");

  // 1) delete params of key_type
  // 2) delete files that are not defined in the keys.
  if (DIR *d = opendir(getParamPath().c_str())) {
    struct dirent *de = NULL;
    while ((de = readdir(d))) {
      if (de->d_type != DT_DIR) {
        auto it = keys.find(de->d_name);
        if (it == keys.end() || (it->second & key_type)) {
          unlink(getParamPath(de->d_name).c_str());
        }
      }
    }
    closedir(d);
  }

  fsync_dir(getParamPath());
}

void Params::putNonBlocking(const std::string &key, const std::string &val) {
   queue.push(std::make_pair(key, val));
  // start thread on demand
  if (!future.valid() || future.wait_for(std::chrono::milliseconds(0)) == std::future_status::ready) {
    future = std::async(std::launch::async, &Params::asyncWriteThread, this);
  }
}

void Params::asyncWriteThread() {
  // TODO: write the latest one if a key has multiple values in the queue.
  std::pair<std::string, std::string> p;
  while (queue.try_pop(p, 0)) {
    // Params::put is Thread-Safe
    put(p.first, p.second);
  }
}
