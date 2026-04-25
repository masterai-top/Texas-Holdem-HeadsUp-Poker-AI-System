# ♠️ MasterAI v3.0 | 一对一和一对多无限注德州扑克最强AI|poker bot / game theory AI

> **基于强化学习 + CFR + 深度神经网络 | 在非完美信息博弈中达到超人水平**

[![Contact](https://img.shields.io/badge/联系-TG%3A%40xuzongbin001-blue)](https://t.me/xuzongbin001)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![C++](https://img.shields.io/badge/C%2B%2B-11-red.svg)](https://isocpp.org/)
[![Stars](https://img.shields.io/github/stars/masterai-top/The-strongest-AI-in-Texas-Hold-em-1-to-1?style=social)](https://github.com/masterai-top/The-strongest-AI-in-Texas-Hold-em-1-to-1)

---

## 🏆 项目定位 | Overview

MasterAI v3.0 是从 v1.0 迭代而来的德州扑克AI，在**一对一和一对多无限注（No-Limit）** 德州扑克中达到超人水平。它是非完美信息博弈中**强化学习 + 搜索**技术的重大突破。

| 核心能力 | 说明 |
| :--- | :--- |
| 🎯 **博弈类型** | 一对一和一对多无限注德州扑克 (Heads-Up No-Limit) |
| 🧠 **核心算法** | CFR（反事实遗憾最小化）+ 深度神经网络 |
| 📊 **收敛性** | 在二人零和博弈中逼近纳什均衡 |
| 🔬 **技术地位** | MasterAI v1.0 曾战胜14位中国顶级职业选手 |

## 🧠 核心技术 | Technology

| 技术模块 | 说明 |
| :--- | :--- |
| **自我博弈强化学习** | 通过自我对弈持续进化策略 |
| **CFR算法** | 反事实遗憾最小化，保证收敛至纳什均衡 |
| **深度神经网络** | 评估博弈状态与行动价值 |
| **公共置信状态搜索** | 基于置信状态的单层前瞻搜索 |

### 架构组成

- **离线训练**：使用CFR框架训练神经网络模型
- **在线决策**：连续重解算法，动态选择最优行动

## ⚠️ 技术瓶颈 | Technical Bottlenecks

本项目已解决以下高难度技术问题：

| 瓶颈 | 挑战 |
| :--- | :--- |
| 计算量 | 训练状态空间达 2,560,000 × 1,750 |
| 推理延迟 | 3-5秒/步（已优化） |
| 博弈树规模 | Abstract CFR (400BB) 节点数超4亿 |



**语言占比**：C++ 58.4% | Python 37.6% | 其他 4%
## 🧠 AI Architecture

- Counterfactual Regret Minimization (CFR)
- Opponent Range Estimation
- Action Policy Network
- Game Tree Search
## 📊 Benchmark Results

| Opponent | Winrate |
|----------|--------|
| Random Bot | 85% |
| Rule-based AI | 72% |
| CFR Baseline | 58% |
## 🎮 Demo

- AI vs AI simulation
- 1v1 heads-up matches
- Hand replay system

## 📸 技术架构图

| 训练流程 | 决策架构 |
| :---: | :---: |
 ![微信图片_20241030103018](https://github.com/user-attachments/assets/a68c45e7-a4f5-4241-a85d-0a9cb7a85546)
 <img width="340" height="536" alt="微信图片_20260320173255" src="https://github.com/user-attachments/assets/66851632-7b29-4fc3-a35c-76dee4e5930d" />


> 📷 技术架构图请联系我获取

## 💰 获取源码 | Contact

✅ MasterAI v3.0 完整源代码 (Python + C++)  
✅ 训练好的神经网络模型  
✅ 部署脚本与文档  

德州扑克的项目和源码链接：https://github.com/masterai-top/TexasHoldem-Poker-Complete-Solution

📱 **Telegram：@xuzongbin001**  
📧 **Email：masterai918@gmail.com**

👉 **联系我获取演示 + 详细报价**


⭐ Star 这个仓库，支持世界级德州AI的持续进化！



## Introduction

MasterAI v3.0 is an iterative algorithm derived from MasterAI v1.0 
It utilizes profound Reinforcement Learning + Search in imperfect-information games and achieves superhuman performance in heads-up no-limit Texas Hold’em. Furthermore, it is a major step toward developing technologies for multiagent interactions in real world.
MasterAI v3.0是从MasterAI v1.0衍生出来的迭代算法，它在非完全信息游戏中利用了通用的强化学习+搜索，并在一对一无限押注的德州扑克中实现了超人的表现。此外，这是在现实世界中开发多智能体交互技术的重要一步。可以应用在线下和线上的poke游戏等各种场景；

## Technology

1.MaterAI v3.0 algorithm generalizes the paradigm of self-play reinforcement learning and deep learning and search through gargantuan imperfect-information. It makes decisions by factoring in the probability distribution of different beliefs each player might have about the current state of the game and uses counterfactual Regret minimization (CFR) algorithm to search efficiently.


2.Our experiments confirmed that MasterAI does indeed converge to an approximate Nash equilibrium in two-player zero-zum game

## Technical bottlenecks

Some technical bottlenecks are encountered when training the algorithm model with CFR framework. For instance, the large state space is leading to too much computation:

1.Algorithm training has a large amount of calculation (2560000 * 1750 in the paper)

2.Deployment speculation and search time is too much: 3 ~ 5 seconds

3.The number of nodes in Abstract CFR (400BB) Betting Tree is too large, more than 400 million



The Master team is constantly exploring the innovation of AI algorithm, and hoping that like-minded technical experts from all over the world can communicate and exchange here, or join us to make MasterAI bigger and stronger together. Please feel free to contact us at Telegram：@alibaba401
