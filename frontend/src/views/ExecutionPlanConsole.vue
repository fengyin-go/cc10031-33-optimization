<template>
  <section class="page" data-module="execution-plan-console">
    <header class="page-head">
      <div>
        <h2>执行计划控制台</h2>
        <p class="page-desc">
          执行计划与稳定步骤键共同唯一，重复提交复用任务编号；中断恢复只续做未完成步骤，已完成/已取消保留清晰终态。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" data-testid="refresh-plans" @click="reload">刷新待办</button>
      </div>
    </header>

    <!-- 业务入口：提交执行计划 -->
    <form class="plan-submit card" @submit.prevent="submitPlan()">
      <h3>提交执行计划</h3>
      <div class="form-grid">
        <label class="filter-item">
          <span>流程 ID</span>
          <input v-model.number="form.bot_id" data-testid="form-bot-id" type="number" min="1" required />
        </label>
        <label class="filter-item">
          <span>流程版本</span>
          <input v-model="form.bot_version" data-testid="form-bot-version" placeholder="如 1.0.0" required />
        </label>
        <label class="filter-item">
          <span>版本冲突处理边界</span>
          <select v-model="form.conflict_policy" data-testid="form-policy">
            <option value="cancel_old">最新版本取消旧版本未执行项</option>
            <option value="coexist">两个版本计划各自完成</option>
          </select>
        </label>
        <label class="filter-item">
          <span>步骤数</span>
          <input v-model.number="form.step_count" data-testid="form-step-count" type="number" min="1" max="20" required />
        </label>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="submit-plan" type="submit" :disabled="submitting">
          {{ submitting ? '提交中…' : '提交并刷新待办' }}
        </button>
        <button class="btn ghost" type="button" data-testid="duplicate-submit" :disabled="submitting" @click="submitPlan(true)">
          模拟重复提交
        </button>
      </div>
      <p v-if="lastSubmit" class="hint" data-testid="submit-summary">
        计划 {{ lastSubmit.plan_key }}：{{ lastSubmit.reused ? '复用既有编号（幂等）' : '新建排队项' }}，
        步骤 {{ lastSubmit.total_steps }} 个，剩余待办 {{ lastTodo }} 项
      </p>
    </form>

    <p v-if="message" class="hint" :class="messageKind" data-testid="console-message">{{ message }}</p>

    <!-- 步骤完成边界总览 -->
    <h3>计划步骤完成边界</h3>
    <table class="data-table" data-testid="plan-table">
      <thead>
        <tr>
          <th>执行计划</th>
          <th>流程版本</th>
          <th>步骤总数</th>
          <th>已完成边界</th>
          <th>排队/执行中</th>
          <th>已取消</th>
          <th>失败</th>
          <th>计划状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="plan in plans" :key="plan.plan_key" :data-testid="`plan-row-${plan.plan_key}`">
          <td>{{ plan.plan_key }}</td>
          <td>{{ plan.bot_version }}</td>
          <td>{{ plan.total_steps }}</td>
          <td data-testid="completed-boundary">连续完成至第 {{ plan.completion_boundary_seq }} 步（{{ plan.completed }}）</td>
          <td>{{ plan.remaining }}</td>
          <td>{{ plan.canceled }}</td>
          <td>{{ plan.failed }}</td>
          <td>{{ plan.plan_canceled ? '已被新版本取代' : '有效' }}</td>
          <td class="row-actions">
            <button class="link" type="button" data-testid="view-detail" @click="openDetail(plan.plan_key)">处理详情</button>
            <button class="link" type="button" data-testid="recover-plan" @click="recover(plan.plan_key)">中断恢复</button>
            <button class="link" type="button" data-testid="cancel-plan" @click="cancelPlan(plan.plan_key)">取消未执行项</button>
          </td>
        </tr>
        <tr v-if="!plans.length">
          <td colspan="9" class="empty-state" data-testid="plan-empty">暂无执行计划，可先提交一个计划</td>
        </tr>
      </tbody>
    </table>

    <!-- 处理详情与结果清单 -->
    <div v-if="detail" class="card" data-testid="plan-detail">
      <h3>处理详情与结果清单：{{ detail.plan_key }}</h3>
      <p class="hint">
        恢复后列表只出现未完成项；已完成、已取消任务保留终态。当前连续完成边界：第
        {{ detail.completion_boundary_seq }} 步
      </p>
      <table class="data-table">
        <thead>
          <tr>
            <th>步骤键</th>
            <th>序号</th>
            <th>任务编号</th>
            <th>当前状态</th>
            <th>终态</th>
            <th>版本快照</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="boundary in detail.boundaries" :key="boundary.step_key" :data-status="boundary.status">
            <td>{{ boundary.step_key }}</td>
            <td>{{ boundary.seq }}</td>
            <td>{{ boundary.任务编号 ?? '—' }}</td>
            <td>{{ boundary.status }}</td>
            <td>{{ boundary.terminal ? '已终态' : '待处理' }}</td>
            <td>
              <button
                v-if="boundary.任务编号"
                class="link"
                type="button"
                :data-testid="`snapshot-${boundary.seq}`"
                @click="checkSnapshot(boundary)"
              >
                核对快照
              </button>
              <span v-else>—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- 失败清单与重试 -->
    <h3>失败任务与中断异常</h3>
    <table class="data-table" data-testid="failure-table">
      <thead>
        <tr>
          <th>任务编号</th>
          <th>执行计划</th>
          <th>步骤键</th>
          <th>失败原因</th>
          <th>已重试次数</th>
          <th>终态时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="failure in failures" :key="failure.task_id" data-testid="failure-row">
          <td>{{ failure.任务编号 }}</td>
          <td>{{ failure.plan_key }}</td>
          <td>{{ failure.step_key }}</td>
          <td>{{ failure.失败原因 }}</td>
          <td>{{ failure.retry_count }}</td>
          <td>{{ failure.终态时间 }}</td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`retry-${failure.task_id}`" @click="retry(failure.task_id)">
              重试（按原因重新排队）
            </button>
          </td>
        </tr>
        <tr v-if="!failures.length">
          <td colspan="7" class="empty-state" data-testid="failure-empty">空态暂无失败</td>
        </tr>
      </tbody>
    </table>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  cancelExecutionPlan,
  loadPlanBoundaries,
  loadPlanFailures,
  loadPlanProgress,
  loadTaskSnapshot,
  recoverPlan as recoverPlanApi,
  retryFailedTask,
  submitExecutionPlan,
} from '@/api/backend-service'

type Boundary = {
  step_key: string
  seq: number
  任务编号: number | null
  status: string
  terminal: boolean
}

type PlanBoundary = {
  ok: boolean
  plan_key: string
  bot_version: string
  plan_canceled: boolean
  total_steps: number
  completed: number
  canceled: number
  failed: number
  remaining: number
  completion_boundary_seq: number
  boundaries: Boundary[]
}

type FailureRow = {
  task_id: number
  任务编号: string
  plan_key: string
  step_key: string
  失败原因: string
  retry_count: number
  终态时间: string
}

const plans = ref<PlanBoundary[]>([])
const failures = ref<FailureRow[]>([])
const detail = ref<PlanBoundary | null>(null)
const submitting = ref(false)
const message = ref('')
const messageKind = ref('hint')
const lastSubmit = ref<Record<string, any> | null>(null)
const lastTodo = ref(0)

const form = ref({
  bot_id: 9001,
  bot_version: '1.0.0',
  conflict_policy: 'cancel_old' as 'cancel_old' | 'coexist',
  step_count: 5,
})

function flash(text: string, ok = true) {
  message.value = text
  messageKind.value = ok ? 'hint' : 'error-text'
}

function buildSteps(count: number) {
  const templates = [
    { name: '打开目标页面', type: 'navigate', element: '#login-btn' },
    { name: '输入关键词', type: 'input', element: '#search-input' },
    { name: '点击搜索', type: 'click', element: '#search-btn' },
    { name: '提取结果表格', type: 'extract', element: '.results-table' },
    { name: '翻页', type: 'click', element: '.next-page' },
  ]
  return Array.from({ length: count }, (_, index) => ({
    seq: index + 1,
    ...(templates[index % templates.length]),
  }))
}

async function submitPlan(duplicate = false) {
  submitting.value = true
  message.value = ''
  try {
    const payload = {
      bot_id: form.value.bot_id,
      bot_version: form.value.bot_version,
      conflict_policy: form.value.conflict_policy,
      steps: buildSteps(form.value.step_count),
    }
    const data = await submitExecutionPlan(payload)
    lastSubmit.value = data.submit
    lastTodo.value = data.todo.remaining
    if (duplicate) {
      const again = await submitExecutionPlan(payload)
      flash(
        again.submit.reused
          ? `并发/重复提交只算一次：${again.submit.plan_key} 复用全部任务编号，新增排队项 ${again.queue.generated} 条`
          : '重复提交未命中幂等，请检查计划键',
        again.submit.reused,
      )
    } else {
      flash(
        `提交成功：生成 ${data.queue.generated} 条、复用 ${data.queue.reused ?? 0} 条、冲突取消 ${data.queue.canceled_by_conflict ?? 0} 条`,
      )
    }
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '执行计划提交失败', false)
  } finally {
    submitting.value = false
  }
}

async function openDetail(planKey: string) {
  try {
    detail.value = (await loadPlanProgress(planKey)) as PlanBoundary
  } catch (error) {
    flash(error instanceof Error ? error.message : '处理详情读取失败', false)
  }
}

async function recover(planKey: string) {
  try {
    const result = await recoverPlanApi(planKey)
    flash(
      `中断恢复完成：回队在途 ${result.requeued_in_flight.length} 条、补建缺失 ${result.rebuilt_missing.length} 条，已完成步骤未重新入队`,
    )
    await reload()
    detail.value = (await loadPlanProgress(planKey)) as PlanBoundary
  } catch (error) {
    flash(error instanceof Error ? error.message : '中断恢复失败', false)
  }
}

async function cancelPlan(planKey: string) {
  try {
    const result = await cancelExecutionPlan(planKey)
    flash(`计划 ${planKey} 已取消未执行项 ${result.canceled.length} 条，已完成项保留终态`)
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '计划取消失败', false)
  }
}

async function retry(taskId: number) {
  try {
    const result = await retryFailedTask(taskId)
    flash(result.ok ? result.message : result.error, result.ok)
    await reload()
  } catch (error) {
    flash(error instanceof Error ? error.message : '重试失败', false)
  }
}

async function checkSnapshot(boundary: Boundary) {
  if (!boundary.任务编号) return
  try {
    const result = await loadTaskSnapshot(boundary.任务编号)
    flash(
      result.ok
        ? `步骤 ${boundary.step_key} 流程版本快照核对一致`
        : `步骤 ${boundary.step_key} 快照漂移字段：${result.mismatch_fields.join('、')}`,
      result.ok,
    )
  } catch (error) {
    flash(error instanceof Error ? error.message : '快照核对失败', false)
  }
}

async function reload() {
  try {
    const [planData, failureData] = await Promise.all([loadPlanBoundaries(), loadPlanFailures()])
    plans.value = planData.plans
    failures.value = failureData.failures
    if (detail.value) {
      detail.value = (await loadPlanProgress(detail.value.plan_key)) as PlanBoundary
    }
  } catch (error) {
    flash(error instanceof Error ? error.message : '待办刷新失败', false)
  }
}

onMounted(reload)
</script>
