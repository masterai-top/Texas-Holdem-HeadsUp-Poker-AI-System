markdown
“Not for illegal gambling. For research only
# ♠️ MasterAI v3.0 |德州扑克AI系统（CFR + 强化学习 + 深度神经网络）|德州AI |德州AI模型|德州扑克AI研究项目 |德州||poker bot  AI| game theory AI

> **基于强化学习 + CFR + 深度神经网络 | 在非完美信息博弈中达到超人水平 | 研究用途，禁止用于非法赌博**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)技术沟通
[![Stars](https://img.shields.io/github/stars/masterai-top/MasterAI-Poker-CFR?style=social)](https://github.com/masterai-top/MasterAI-Poker-CFR)
[![C++](https://img.shields.io/badge/C++-57.4%25-blue)](https://isocpp.org/)
[![Python](https://img.shields.io/badge/Python-36.2%25-green)](https://python.org/)

**🇨🇳 中文 | [English](README_EN.md) (欢迎贡献英文版)**


---

## 🎯 项目简介 | What is MasterAI v3.0?

MasterAI v3.0 是一个**超人类水平**的**一对多无上限德州扑克（Heads-Up No-Limit Texas Hold'em）AI**。它结合了：

- **反事实遗憾最小化（CFR，Counterfactual Regret Minimization）**：理论保证收敛至纳什均衡
- **深度神经网络（Deep Neural Networks）**：评估博弈状态与行动价值
- **自我博弈强化学习（Self-Play Reinforcement Learning）**：通过自我对弈持续进化

**MasterAI v1.0 在外部测试中与多名高水平人类玩家进行了对局，用于评估策略收敛性与胜率表现。**
**MasterAI v3.0 在训练效率和决策质量上均有大幅提升**

## 🧠 核心技术 | Core Technology

| 模块 | 技术方案 |
|------|----------|
| **核心算法** | CFR + 深度强化学习 |
| **博弈类型** | 一对一无上限（HUNL） |
| **理论保证** | 二人零和博弈中收敛至纳什均衡 |
| **在线搜索** | 公共置信状态（PBS）+ 单层前瞻搜索 |
| **系统架构** | 离线训练（CFR框架）+ 在线连续重解 |

### 架构组成

- **离线训练**：使用 CFR 框架训练神经网络模型
- **在线决策**：连续重解算法，动态选择最优行动

## 📊 基准测试结果 | Benchmark Results

| 对手类型 | 胜率 |
|----------|------|
| 随机机器人（Random Bot） | 85% |
| 基于规则的AI（Rule-based AI） | 72% |
| CFR基线模型（CFR Baseline） | 58% |

## ⚙️ 解决的技术难点 | Technical Challenges Solved

| 难点 | 挑战 | 我们的解决方案 |
|------|------|----------------|
| **计算量** | 训练状态空间达 2,560,000 × 1,750 | 状态抽象与剪枝 |
| **推理延迟** | 3-5秒/步 | 优化至 <0.5秒/步 |
| **博弈树规模** | Abstract CFR (400BB) 节点数超4亿 | 高效遗憾匹配算法 |

**代码语言占比**：C++ 57.4% | Python 36.2% | 其他 6.4%

## 📁 项目结构 | Repository Structure

```
├── csrc/           # C++ 核心代码（CFR、博弈树、进程间通信）
├── game/           # 扑克游戏逻辑与规则
├── cfv/            # 反事实价值计算（Counterfactual Value）
├── rela/           # 强化学习智能体
├── robot/          # 对手AI模型
├── tests/          # 单元测试与基准测试
├── docs/           # 文档（训练教程、API参考）
├── main.py         # 训练入口
├── run.py          # 运行AI对战
├── train.sh        # 训练脚本
└── deploy.sh       # 部署辅助脚本
```

## 🚀 快速开始 | Quick Start（Linux/macOS）

### 环境要求
- Python 3.8+
- C++ 编译器（支持 C++17）
- CMake 3.10+（可选）

### 克隆与编译

```bash
# 克隆仓库
git clone https://github.com/masterai-top/MasterAI-Poker-CFR.git
cd MasterAI-Poker-CFR

# 安装Python依赖（请根据实际依赖补充）
pip install numpy torch

# 编译C++模块（如需）
mkdir build && cd build
cmake .. && make
```

### 运行AI对战

```bash
# AI vs 随机机器人（1000手牌）
python run.py --opponent random --hands 1000

# AI vs 基于规则的AI
python run.py --opponent rule_based --hands 500

# AI vs AI（自我博弈）
python run.py --self_play --hands 100
```

### 训练自己的模型

```bash
# 注意：完整训练需要高性能计算资源，通常需要数天至数周
./train.sh --iterations 1000 --save_model ./my_model.pkl
```

> ⚠️ **提示**：完整训练建议使用GPU集群。。

## 📈 性能指标 | Performance Metrics

- **决策时间**：约 0.4秒/步（单GPU）
- **内存占用**：400BB博弈树约 8GB
- **收敛速度**：自我博弈约2周后逼近纳什均衡




📷 **产品说明** – 

 <img width="340" height="536" alt="微信图片_20260320173255" src="https://github.com/user-attachments/assets/66851632-7b29-4fc3-a35c-76dee4e5930d" />
<img width="1050" height="706" alt="微信图片_20241030103520" src="https://github.com/user-attachments/assets/5fa3a0e8-0326-48a1-944a-bf497f0027ac" />
<img width="1080" height="644" alt="640 (1)" src="https://github.com/user-attachments/assets/8cba978a-8a2f-4310-b650-d96adf1dd633" />
<img width="379" height="447" alt="微信图片_20241030112757" src="https://github.com/user-attachments/assets/3979385c-cebc-47df-aadf-a317dd3eb021" />




###  问题反馈与交流
仅限学术与技术交流
- **Telegram**：[@xuzongbin001](https://t.me/xuzongbin001)
- **Email**：masterai918@gmail.com
- **相关项目**：[TexasHoldem-Poker-Complete-Solution](https://github.com/masterai-top/TexasHoldem-Poker-Complete-Solution)

### 开源协议

本项目采用 **Apache 2.0 协议** 。

## 🤝 参与贡献 | Contributing

我们欢迎博弈AI、强化学习、非完美信息博弈领域的研究者和工程师参与贡献：

- 算法改进（CFR+、Deep CFR、NFSP等）
- 训练效率优化
- 多智能体扩展（6人桌、满员桌）
- 文档与教程翻译



## 📖 引用 | Citation

如果在研究中使用本代码，请引用：

```bibtex
@software{masterai2024cfr,
  author = {MasterAI Team},
  title = {MasterAI v3.0: 基于CFR的德州扑克AI系统},
  year = {2024},
  url = {https://github.com/masterai-top/MasterAI-Poker-CFR}
}
```

## ⭐ 支持我们

如果这个项目对你有帮助，请点亮右上角的 **Star**，让更多人看到世界级的德州扑克AI技术！

---





## Introduction

MasterAI v3.0 is an iterative algorithm derived from MasterAI v1.0 
It utilizes profound Reinforcement Learning + Search in imperfect-information games and achieves superhuman performance in heads-up no-limit Texas Hold’em. Furthermore, it is a major step toward developing technologies for multiagent interactions in real world.
MasterAI v3.0是从MasterAI v1.0衍生出来的迭代算法，它在非完全信息游戏中利用了通用的强化学习+搜索，并在一对多无限押注的德州扑克中实现了超人的表现。此外，这是在现实世界中开发多智能体交互技术的重要一步。可以应用在线下和线上的poke游戏等各种场景；

## Technology

1.MaterAI v3.0 algorithm generalizes the paradigm of self-play reinforcement learning and deep learning and search through gargantuan imperfect-information. It makes decisions by factoring in the probability distribution of different beliefs each player might have about the current state of the game and uses counterfactual Regret minimization (CFR) algorithm to search efficiently.


2.Our experiments confirmed that MasterAI does indeed converge to an approximate Nash equilibrium in two-player zero-zum game

## Technical bottlenecks

Some technical bottlenecks are encountered when training the algorithm model with CFR framework. For instance, the large state space is leading to too much computation:

1.Algorithm training has a large amount of calculation (2560000 * 1750 in the paper)

2.Deployment speculation and search time is too much: 3 ~ 5 seconds

3.The number of nodes in Abstract CFR (400BB) Betting Tree is too large, more than 400 million



