import "dotenv/config";
import { Kafka, CompressionTypes, ConfigResourceTypes, Admin, Producer } from "kafkajs";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

// ── Environment Variables ──────────────────────────────────────────────
const BOOTSTRAP_SERVERS = process.env.KAFKA_BOOTSTRAP_SERVERS ?? "";
const API_KEY = process.env.KAFKA_API_KEY ?? "";
const API_SECRET = process.env.KAFKA_API_SECRET ?? "";
const GROUP_ID = process.env.KAFKA_GROUP_ID ?? "mcp-consumer-group";

// ── Kafka Client ───────────────────────────────────────────────────────
const kafka = new Kafka({
  clientId: "mcp-kafka-server",
  brokers: BOOTSTRAP_SERVERS.split(","),
  ssl: true,
  sasl: {
    mechanism: "plain",
    username: API_KEY,
    password: API_SECRET,
  },
});

// ── Lazy-connected singletons ──────────────────────────────────────────
let adminClient: Admin | null = null;
let producerClient: Producer | null = null;

async function getAdmin(): Promise<Admin> {
  if (!adminClient) {
    adminClient = kafka.admin();
    await adminClient.connect();
  }
  return adminClient;
}

async function getProducer(): Promise<Producer> {
  if (!producerClient) {
    producerClient = kafka.producer();
    await producerClient.connect();
  }
  return producerClient;
}

// ── MCP Server ─────────────────────────────────────────────────────────
const server = new McpServer({
  name: "confluent-kafka",
  version: "1.0.0",
});

// ── Tool 1: list_topics ────────────────────────────────────────────────
server.tool(
  "list_topics",
  "Kafka 클러스터의 토픽 목록을 조회합니다",
  {},
  async () => {
    try {
      const admin = await getAdmin();
      const metadata = await admin.fetchTopicMetadata();
      const topics = metadata.topics.map((t) => ({
        name: t.name,
        partitions: t.partitions.length,
      }));
      return {
        content: [{ type: "text" as const, text: JSON.stringify(topics, null, 2) }],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    }
  },
);

// ── Tool 2: describe_topic ─────────────────────────────────────────────
server.tool(
  "describe_topic",
  "특정 토픽의 상세 정보(파티션, 오프셋, 설정)를 조회합니다",
  { topic: z.string() },
  async ({ topic }) => {
    try {
      const admin = await getAdmin();

      const [metadata, offsets, configs] = await Promise.all([
        admin.fetchTopicMetadata({ topics: [topic] }),
        admin.fetchTopicOffsets(topic),
        admin.describeConfigs({
          resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }],
          includeSynonyms: false,
        }),
      ]);

      const result = {
        topic,
        partitions: metadata.topics[0]?.partitions.map((p) => ({
          id: p.partitionId,
          leader: p.leader,
          replicas: p.replicas,
          isr: p.isr,
        })),
        offsets: offsets.map((o) => ({
          partition: o.partition,
          high: o.high,
          low: o.low,
        })),
        configs: configs.resources[0]?.configEntries.map((c) => ({
          name: c.configName,
          value: c.configValue,
        })),
      };

      return {
        content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    }
  },
);

// ── Tool 3: produce_message ────────────────────────────────────────────
server.tool(
  "produce_message",
  "Kafka 토픽에 메시지를 전송합니다",
  {
    topic: z.string(),
    messages: z.array(
      z.object({
        key: z.string().optional(),
        value: z.string(),
        headers: z.record(z.string(), z.string()).optional(),
      }),
    ),
  },
  async ({ topic, messages }) => {
    try {
      const producer = await getProducer();
      const result = await producer.send({
        topic,
        compression: CompressionTypes.GZIP,
        messages: messages.map((m) => ({
          key: m.key ?? null,
          value: m.value,
          headers: m.headers,
        })),
      });

      return {
        content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    }
  },
);

// ── Tool 4: consume_messages ───────────────────────────────────────────
server.tool(
  "consume_messages",
  "토픽에서 최근 메시지를 일회성으로 폴링합니다",
  {
    topic: z.string(),
    maxMessages: z.number().default(10),
    fromBeginning: z.boolean().default(false),
  },
  async ({ topic, maxMessages, fromBeginning }) => {
    const tempGroupId = `${GROUP_ID}-${Date.now()}`;
    const consumer = kafka.consumer({ groupId: tempGroupId });

    try {
      await consumer.connect();
      await consumer.subscribe({ topics: [topic], fromBeginning });

      const collected: Array<{
        partition: number;
        offset: string;
        key: string | null;
        value: string | null;
        timestamp: string;
        headers: Record<string, string>;
      }> = [];

      let resolveConsume: () => void;
      const donePromise = new Promise<void>((resolve) => {
        resolveConsume = resolve;
      });

      const timeout = setTimeout(() => {
        resolveConsume();
      }, 10_000);

      await consumer.run({
        eachMessage: async ({ partition, message }) => {
          const headers: Record<string, string> = {};
          if (message.headers) {
            for (const [k, v] of Object.entries(message.headers)) {
              if (Buffer.isBuffer(v)) {
                headers[k] = v.toString();
              } else if (typeof v === "string") {
                headers[k] = v;
              }
            }
          }

          collected.push({
            partition,
            offset: message.offset,
            key: message.key?.toString() ?? null,
            value: message.value?.toString() ?? null,
            timestamp: message.timestamp,
            headers,
          });

          if (collected.length >= maxMessages) {
            resolveConsume();
          }
        },
      });

      await donePromise;
      clearTimeout(timeout);

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(
              { groupId: tempGroupId, count: collected.length, messages: collected },
              null,
              2,
            ),
          },
        ],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    } finally {
      await consumer.disconnect();
    }
  },
);

// ── Tool 5: create_topic ───────────────────────────────────────────────
server.tool(
  "create_topic",
  "새 Kafka 토픽을 생성합니다",
  {
    topic: z.string(),
    numPartitions: z.number().default(6),
    replicationFactor: z.number().default(3),
    configs: z.record(z.string(), z.string()).optional(),
  },
  async ({ topic, numPartitions, replicationFactor, configs }) => {
    try {
      const admin = await getAdmin();
      const created = await admin.createTopics({
        topics: [
          {
            topic,
            numPartitions,
            replicationFactor,
            configEntries: configs
              ? Object.entries(configs).map(([name, value]) => ({ name, value }))
              : undefined,
          },
        ],
      });

      const message = created
        ? `토픽 '${topic}'이(가) 생성되었습니다 (파티션: ${numPartitions}, 복제 팩터: ${replicationFactor}).`
        : `토픽 '${topic}'이(가) 이미 존재합니다.`;

      return {
        content: [{ type: "text" as const, text: message }],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    }
  },
);

// ── Tool 6: list_consumer_groups ───────────────────────────────────────
server.tool(
  "list_consumer_groups",
  "Kafka 컨슈머 그룹 목록을 조회합니다",
  {},
  async () => {
    try {
      const admin = await getAdmin();
      const { groups } = await admin.listGroups();
      return {
        content: [{ type: "text" as const, text: JSON.stringify(groups, null, 2) }],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    }
  },
);

// ── Tool 7: describe_consumer_group ────────────────────────────────────
server.tool(
  "describe_consumer_group",
  "컨슈머 그룹의 오프셋과 랙(lag)을 조회합니다",
  { groupId: z.string() },
  async ({ groupId }) => {
    try {
      const admin = await getAdmin();

      const [groupDesc, offsets] = await Promise.all([
        admin.describeGroups([groupId]),
        admin.fetchOffsets({ groupId }),
      ]);

      const group = groupDesc.groups[0];

      // Calculate lag for each topic/partition
      const offsetsWithLag = await Promise.all(
        offsets.map(async (topicOffset) => {
          try {
            const latestOffsets = await admin.fetchTopicOffsets(topicOffset.topic);
            const partitions = topicOffset.partitions.map((p) => {
              const latest = latestOffsets.find((lo) => lo.partition === p.partition);
              const currentOffset = parseInt(p.offset, 10);
              const highOffset = latest ? parseInt(latest.high, 10) : 0;
              const lag = currentOffset >= 0 && highOffset > 0 ? highOffset - currentOffset : null;
              return { partition: p.partition, offset: p.offset, lag };
            });
            return { topic: topicOffset.topic, partitions };
          } catch {
            // If fetching latest offsets fails, return without lag
            return {
              topic: topicOffset.topic,
              partitions: topicOffset.partitions.map((p) => ({
                partition: p.partition,
                offset: p.offset,
                lag: null,
              })),
            };
          }
        }),
      );

      const result = {
        groupId: group?.groupId,
        state: group?.state,
        protocol: group?.protocol,
        members: group?.members.map((m) => ({
          memberId: m.memberId,
          clientId: m.clientId,
          clientHost: m.clientHost,
        })),
        offsets: offsetsWithLag,
      };

      return {
        content: [{ type: "text" as const, text: JSON.stringify(result, null, 2) }],
      };
    } catch (err) {
      return {
        isError: true,
        content: [{ type: "text" as const, text: `❌ 오류: ${(err as Error).message}` }],
      };
    }
  },
);

// ── Resource: cluster-info ─────────────────────────────────────────────
server.resource(
  "cluster-info",
  "kafka://cluster/info",
  { mimeType: "application/json" },
  async () => {
    const admin = await getAdmin();
    const cluster = await admin.describeCluster();
    return {
      contents: [
        {
          uri: "kafka://cluster/info",
          mimeType: "application/json",
          text: JSON.stringify(cluster, null, 2),
        },
      ],
    };
  },
);

// ── Graceful Shutdown ──────────────────────────────────────────────────
async function shutdown() {
  console.error("Shutting down...");
  try {
    if (producerClient) await producerClient.disconnect();
    if (adminClient) await adminClient.disconnect();
  } catch (err) {
    console.error("Error during shutdown:", err);
  }
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// ── Start Server ───────────────────────────────────────────────────────
async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("Confluent Kafka MCP Server running on stdio");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
