import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

// ---- 执行计划幂等与失败恢复 ----

export type PlanStepInput = {
  seq: number
  name: string
  type?: string
  element?: string
  params?: string
  wait?: string
  on_error?: string
}

export type PlanSubmitInput = {
  bot_id: number
  steps: PlanStepInput[]
  bot_version?: string
  conflict_policy?: 'cancel_old' | 'coexist'
}

/** 业务入口：幂等提交执行计划，联动刷新待办与步骤完成边界 */
export async function submitExecutionPlan(input: PlanSubmitInput): Promise<Record<string, any>> {
  const response = await request('/api/execution-plans/submit', {
    method: 'POST',
    body: JSON.stringify(input),
  })
  if (!response.ok) throw new Error('执行计划提交失败')
  return await response.json()
}

/** 按流程版本和计划查看步骤完成边界 */
export async function loadPlanBoundaries(botId?: number): Promise<Record<string, any>> {
  const query = botId ? `?bot_id=${botId}` : ''
  const response = await request(`/api/execution-plans${query}`)
  if (!response.ok) throw new Error('执行计划边界读取失败')
  return await response.json()
}

/** 单个计划的步骤完成边界 */
export async function loadPlanProgress(planKey: string): Promise<Record<string, any>> {
  const response = await request(`/api/execution-plans/${encodeURIComponent(planKey)}/progress`)
  if (!response.ok) throw new Error('计划进度读取失败')
  return await response.json()
}

/** 中断恢复：只续做未完成任务 */
export async function recoverPlan(planKey: string): Promise<Record<string, any>> {
  const response = await request(`/api/execution-plans/${encodeURIComponent(planKey)}/recover`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('中断恢复失败')
  return await response.json()
}

/** 取消计划未执行项 */
export async function cancelExecutionPlan(planKey: string): Promise<Record<string, any>> {
  const response = await request(`/api/execution-plans/${encodeURIComponent(planKey)}/cancel`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('计划取消失败')
  return await response.json()
}

/** 失败清单（空态返回空列表） */
export async function loadPlanFailures(): Promise<Record<string, any>> {
  const response = await request('/api/execution-plans/failures/list')
  if (!response.ok) throw new Error('失败清单读取失败')
  return await response.json()
}

/** 重试中断异常任务 */
export async function retryFailedTask(taskId: number): Promise<Record<string, any>> {
  const response = await request(`/api/execution-plans/tasks/${taskId}/retry`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('失败任务重试失败')
  return await response.json()
}

/** 核对处理项对应的流程版本快照 */
export async function loadTaskSnapshot(taskId: number): Promise<Record<string, any>> {
  const response = await request(`/api/execution-plans/tasks/${taskId}/snapshot`)
  if (!response.ok) throw new Error('版本快照核对失败')
  return await response.json()
}
