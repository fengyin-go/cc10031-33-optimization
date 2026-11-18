<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">RPA流程自动化中心</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向企业数据采集场景，提供RPA流程编排、元素定位、执行调度和结果校验的自动化运营平台</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "执行计划控制台", path: "/execution-plans" }, { label: "流程定义", path: "/bot_definition" }, { label: "步骤编排", path: "/step_editor" }, { label: "元素定位", path: "/element_selector" }, { label: "触发配置", path: "/trigger_config" }, { label: "执行记录", path: "/execution_log" }, { label: "数据提取", path: "/data_extract" }, { label: "凭据管理", path: "/credential_store" }, { label: "环境配置", path: "/env_profile" }, { label: "反封锁", path: "/anti_block" }, { label: "结果校验", path: "/result_verify" }, { label: "批次追踪", path: "/batch_tracker" }, { label: "页面建模", path: "/page_model" }, { label: "数据补全", path: "/data_enrich" }, { label: "代理路由", path: "/proxy_router" }, { label: "频控策略", path: "/rate_limiter" }, { label: "输出格式", path: "/output_format" }, { label: "异常日志", path: "/anomaly_log" }, { label: "调度计划", path: "/scheduler" }, { label: "运行概览", path: "/report_dashboard" }, { label: "任务队列", path: "/task_queue" }]
</script>
