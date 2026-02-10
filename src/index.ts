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

// Naver Datalab API
const NAVER_CLIENT_ID = process.env.NAVER_CLIENT_ID ?? "";
const NAVER_CLIENT_SECRET = process.env.NAVER_CLIENT_SECRET ?? "";

// ── Naver Datalab API Constants ───────────────────────────────────────
const NAVER_DATALAB_TREND_URL = "https://openapi.naver.com/v1/datalab/search";
const NAVER_DATALAB_SHOPPING_URL = "https://openapi.naver.com/v1/datalab/shopping/categories";
const NAVER_TREND_TOPIC = "naver-datalab-trends";
const NAVER_SHOPPING_TREND_TOPIC = "naver-datalab-shopping";

interface NaverKeywordGroup {
  groupName: string;
  keywords: string[];
}

interface NaverTrendRequest {
  startDate: string;
  endDate: string;
  timeUnit: "date" | "week" | "month";
  keywordGroups: NaverKeywordGroup[];
  device?: "pc" | "mo" | "";
  gender?: "m" | "f" | "";
  ages?: string[];
}

interface NaverShoppingRequest {
  startDate: string;
  endDate: string;
  timeUnit: "date" | "week" | "month";
  category: string;
  device?: "pc" | "mo" | "";
  gender?: "m" | "f" | "";
  ages?: string[];
}

async function callNaverApi<T>(url: string, body: object): Promise<T> {
  if (!NAVER_CLIENT_ID || !NAVER_CLIENT_SECRET) {
    throw new Error("NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET 환경변수를 설정해주세요.");
  }

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "X-Naver-Client-Id": NAVER_CLIENT_ID,
      "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Naver API 오류 (${response.status}): ${errorText}`);
  }

  return response.json() as Promise<T>;
}

// ── Naver Search API ──────────────────────────────────────────────────
type NaverSearchType = "news" | "blog" | "shop" | "image" | "webkr" | "cafearticle";
const NAVER_SEARCH_BASE_URL = "https://openapi.naver.com/v1/search";
const NAVER_SEARCH_TOPIC = "naver-search-results";

interface NaverSearchParams {
  query: string;
  display?: number;
  start?: number;
  sort?: "sim" | "date";
}

async function callNaverSearchApi<T>(type: NaverSearchType, params: NaverSearchParams): Promise<T> {
  if (!NAVER_CLIENT_ID || !NAVER_CLIENT_SECRET) {
    throw new Error("NAVER_CLIENT_ID와 NAVER_CLIENT_SECRET 환경변수를 설정해주세요.");
  }

  const url = new URL(`${NAVER_SEARCH_BASE_URL}/${type}.json`);
  url.searchParams.set("query", params.query);
  if (params.display) url.searchParams.set("display", String(params.display));
  if (params.start) url.searchParams.set("start", String(params.start));
  if (params.sort) url.searchParams.set("sort", params.sort);

  const response = await fetch(url.toString(), {
    method: "GET",
    headers: {
      "X-Naver-Client-Id": NAVER_CLIENT_ID,
      "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    },
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Naver 검색 API 오류 (${response.status}): ${errorText}`);
  }

  return response.json() as Promise<T>;
}

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

// ── Tool 8: naver_datalab_trend ──────────────────────────────────────
server.tool(
  "naver_datalab_trend",
  "네이버 데이터랩 검색어 트렌드를 조회하고 Kafka 토픽에 저장합니다",
  {
    startDate: z.string().describe("조회 시작일 (yyyy-mm-dd)"),
    endDate: z.string().describe("조회 종료일 (yyyy-mm-dd)"),
    timeUnit: z.enum(["date", "week", "month"]).default("month").describe("구간 단위"),
    keywordGroups: z.array(
      z.object({
        groupName: z.string().describe("키워드 그룹명"),
        keywords: z.array(z.string()).describe("검색 키워드 배열"),
      }),
    ).describe("비교할 키워드 그룹 (최대 5개)"),
    device: z.enum(["pc", "mo", ""]).default("").optional().describe("디바이스 필터"),
    gender: z.enum(["m", "f", ""]).default("").optional().describe("성별 필터"),
    ages: z.array(z.string()).optional().describe("연령대 필터 (예: ['1','2'])"),
    produceToKafka: z.boolean().default(true).describe("Kafka 토픽에 결과를 저장할지 여부"),
  },
  async ({ startDate, endDate, timeUnit, keywordGroups, device, gender, ages, produceToKafka }) => {
    try {
      const requestBody: NaverTrendRequest = {
        startDate,
        endDate,
        timeUnit,
        keywordGroups: keywordGroups.map((g) => ({
          groupName: g.groupName,
          keywords: g.keywords,
        })),
      };
      if (device) requestBody.device = device;
      if (gender) requestBody.gender = gender;
      if (ages && ages.length > 0) requestBody.ages = ages;

      const result = await callNaverApi<object>(NAVER_DATALAB_TREND_URL, requestBody);

      // Produce to Kafka if enabled
      if (produceToKafka) {
        const producer = await getProducer();
        const messageValue = JSON.stringify({
          type: "search_trend",
          requestedAt: new Date().toISOString(),
          params: { startDate, endDate, timeUnit, keywordGroups },
          result,
        });

        await producer.send({
          topic: NAVER_TREND_TOPIC,
          compression: CompressionTypes.GZIP,
          messages: [
            {
              key: keywordGroups.map((g) => g.groupName).join("|"),
              value: messageValue,
              headers: {
                source: "naver-datalab",
                type: "search-trend",
                timeUnit,
              },
            },
          ],
        });

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  status: "success",
                  kafkaTopic: NAVER_TREND_TOPIC,
                  message: `트렌드 데이터를 조회하고 '${NAVER_TREND_TOPIC}' 토픽에 저장했습니다.`,
                  data: result,
                },
                null,
                2,
              ),
            },
          ],
        };
      }

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

// ── Tool 9: naver_datalab_shopping ──────────────────────────────────
server.tool(
  "naver_datalab_shopping",
  "네이버 데이터랩 쇼핑인사이트 분야별 트렌드를 조회하고 Kafka 토픽에 저장합니다",
  {
    startDate: z.string().describe("조회 시작일 (yyyy-mm-dd)"),
    endDate: z.string().describe("조회 종료일 (yyyy-mm-dd)"),
    timeUnit: z.enum(["date", "week", "month"]).default("month").describe("구간 단위"),
    category: z.string().describe("쇼핑 카테고리 코드 (예: '50000000')"),
    device: z.enum(["pc", "mo", ""]).default("").optional().describe("디바이스 필터"),
    gender: z.enum(["m", "f", ""]).default("").optional().describe("성별 필터"),
    ages: z.array(z.string()).optional().describe("연령대 필터"),
    produceToKafka: z.boolean().default(true).describe("Kafka 토픽에 결과를 저장할지 여부"),
  },
  async ({ startDate, endDate, timeUnit, category, device, gender, ages, produceToKafka }) => {
    try {
      const requestBody: NaverShoppingRequest = {
        startDate,
        endDate,
        timeUnit,
        category,
      };
      if (device) requestBody.device = device;
      if (gender) requestBody.gender = gender;
      if (ages && ages.length > 0) requestBody.ages = ages;

      const result = await callNaverApi<object>(NAVER_DATALAB_SHOPPING_URL, requestBody);

      if (produceToKafka) {
        const producer = await getProducer();
        const messageValue = JSON.stringify({
          type: "shopping_trend",
          requestedAt: new Date().toISOString(),
          params: { startDate, endDate, timeUnit, category },
          result,
        });

        await producer.send({
          topic: NAVER_SHOPPING_TREND_TOPIC,
          compression: CompressionTypes.GZIP,
          messages: [
            {
              key: category,
              value: messageValue,
              headers: {
                source: "naver-datalab",
                type: "shopping-trend",
                category,
                timeUnit,
              },
            },
          ],
        });

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  status: "success",
                  kafkaTopic: NAVER_SHOPPING_TREND_TOPIC,
                  message: `쇼핑 트렌드 데이터를 조회하고 '${NAVER_SHOPPING_TREND_TOPIC}' 토픽에 저장했습니다.`,
                  data: result,
                },
                null,
                2,
              ),
            },
          ],
        };
      }

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

// ── Tool 10: consume_naver_trends ────────────────────────────────────
server.tool(
  "consume_naver_trends",
  "Kafka에 저장된 네이버 트렌드 데이터를 소비합니다",
  {
    source: z.enum(["search", "shopping"]).default("search").describe("조회할 데이터 소스"),
    maxMessages: z.number().default(10).describe("최대 메시지 수"),
    fromBeginning: z.boolean().default(true).describe("처음부터 읽을지 여부"),
  },
  async ({ source, maxMessages, fromBeginning }) => {
    const topic = source === "shopping" ? NAVER_SHOPPING_TREND_TOPIC : NAVER_TREND_TOPIC;
    const tempGroupId = `${GROUP_ID}-naver-${Date.now()}`;
    const consumer = kafka.consumer({ groupId: tempGroupId });

    try {
      await consumer.connect();
      await consumer.subscribe({ topics: [topic], fromBeginning });

      const collected: Array<{
        partition: number;
        offset: string;
        key: string | null;
        value: object | null;
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

          let parsedValue: object | null = null;
          try {
            parsedValue = message.value ? JSON.parse(message.value.toString()) : null;
          } catch {
            parsedValue = { raw: message.value?.toString() ?? null };
          }

          collected.push({
            partition,
            offset: message.offset,
            key: message.key?.toString() ?? null,
            value: parsedValue,
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
              {
                topic,
                groupId: tempGroupId,
                count: collected.length,
                messages: collected,
              },
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

// ── Tool 11: naver_search ────────────────────────────────────────────
server.tool(
  "naver_search",
  "네이버 검색 API로 뉴스/블로그/쇼핑/이미지/웹/카페 결과를 조회하고 Kafka에 저장합니다",
  {
    query: z.string().describe("검색 키워드"),
    type: z.enum(["news", "blog", "shop", "image", "webkr", "cafearticle"]).default("news").describe("검색 유형"),
    display: z.number().min(1).max(100).default(10).describe("검색 결과 수 (최대 100)"),
    start: z.number().min(1).max(1000).default(1).describe("검색 시작 위치"),
    sort: z.enum(["sim", "date"]).default("date").describe("정렬 (sim: 정확도, date: 날짜)"),
    produceToKafka: z.boolean().default(true).describe("Kafka 토픽에 결과를 저장할지 여부"),
  },
  async ({ query, type, display, start, sort, produceToKafka }) => {
    try {
      const result = await callNaverSearchApi<object>(type, { query, display, start, sort });

      if (produceToKafka) {
        const producer = await getProducer();
        const messageValue = JSON.stringify({
          type: `search_${type}`,
          requestedAt: new Date().toISOString(),
          params: { query, type, display, start, sort },
          result,
        });

        await producer.send({
          topic: NAVER_SEARCH_TOPIC,
          compression: CompressionTypes.GZIP,
          messages: [
            {
              key: `${type}:${query}`,
              value: messageValue,
              headers: {
                source: "naver-search",
                searchType: type,
                query,
                sort,
              },
            },
          ],
        });

        return {
          content: [
            {
              type: "text" as const,
              text: JSON.stringify(
                {
                  status: "success",
                  kafkaTopic: NAVER_SEARCH_TOPIC,
                  message: `'${query}' 검색 결과(${type})를 '${NAVER_SEARCH_TOPIC}' 토픽에 저장했습니다.`,
                  data: result,
                },
                null,
                2,
              ),
            },
          ],
        };
      }

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

// ── Tool 12: naver_search_stream ─────────────────────────────────────
server.tool(
  "naver_search_stream",
  "네이버 검색 결과를 여러 페이지에 걸쳐 수집하고 Kafka에 일괄 저장합니다",
  {
    query: z.string().describe("검색 키워드"),
    type: z.enum(["news", "blog", "shop", "image", "webkr", "cafearticle"]).default("news").describe("검색 유형"),
    totalCount: z.number().min(1).max(500).default(100).describe("총 수집할 결과 수 (최대 500)"),
    sort: z.enum(["sim", "date"]).default("date").describe("정렬"),
  },
  async ({ query, type, totalCount, sort }) => {
    try {
      const pageSize = 100;
      const allItems: unknown[] = [];
      let total = 0;
      let lastBuildDate = "";

      for (let start = 1; start <= totalCount; start += pageSize) {
        const display = Math.min(pageSize, totalCount - start + 1);
        const result = await callNaverSearchApi<{
          lastBuildDate: string;
          total: number;
          items: unknown[];
        }>(type, { query, display, start, sort });

        if (result.items) {
          allItems.push(...result.items);
        }
        total = result.total;
        lastBuildDate = result.lastBuildDate;

        // API rate limit 준수 (100ms 대기)
        if (start + pageSize <= totalCount) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }
      }

      // Kafka에 일괄 produce
      const producer = await getProducer();
      const messages = allItems.map((item, idx) => ({
        key: `${type}:${query}:${idx}`,
        value: JSON.stringify({
          type: `search_${type}`,
          requestedAt: new Date().toISOString(),
          query,
          searchType: type,
          item,
        }),
        headers: {
          source: "naver-search-stream",
          searchType: type,
          query,
          index: String(idx),
        },
      }));

      const sendResult = await producer.send({
        topic: NAVER_SEARCH_TOPIC,
        compression: CompressionTypes.GZIP,
        messages,
      });

      return {
        content: [
          {
            type: "text" as const,
            text: JSON.stringify(
              {
                status: "success",
                kafkaTopic: NAVER_SEARCH_TOPIC,
                message: `'${query}' 검색 결과 ${allItems.length}건을 '${NAVER_SEARCH_TOPIC}' 토픽에 저장했습니다.`,
                summary: {
                  query,
                  type,
                  totalAvailable: total,
                  collected: allItems.length,
                  lastBuildDate,
                },
                kafkaResult: sendResult,
              },
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
    }
  },
);

// ── Tool 13: consume_naver_search ────────────────────────────────────
server.tool(
  "consume_naver_search",
  "Kafka에 저장된 네이버 검색 데이터를 소비합니다",
  {
    maxMessages: z.number().default(10).describe("최대 메시지 수"),
    fromBeginning: z.boolean().default(true).describe("처음부터 읽을지 여부"),
    filterQuery: z.string().optional().describe("특정 검색어로 필터링 (선택)"),
    filterType: z.enum(["news", "blog", "shop", "image", "webkr", "cafearticle"]).optional().describe("특정 검색 유형으로 필터링 (선택)"),
  },
  async ({ maxMessages, fromBeginning, filterQuery, filterType }) => {
    const tempGroupId = `${GROUP_ID}-naver-search-${Date.now()}`;
    const consumer = kafka.consumer({ groupId: tempGroupId });

    try {
      await consumer.connect();
      await consumer.subscribe({ topics: [NAVER_SEARCH_TOPIC], fromBeginning });

      const collected: Array<{
        partition: number;
        offset: string;
        key: string | null;
        value: object | null;
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

          // 필터 적용
          if (filterQuery && headers.query && !headers.query.includes(filterQuery)) {
            return;
          }
          if (filterType && headers.searchType && headers.searchType !== filterType) {
            return;
          }

          let parsedValue: object | null = null;
          try {
            parsedValue = message.value ? JSON.parse(message.value.toString()) : null;
          } catch {
            parsedValue = { raw: message.value?.toString() ?? null };
          }

          collected.push({
            partition,
            offset: message.offset,
            key: message.key?.toString() ?? null,
            value: parsedValue,
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
              {
                topic: NAVER_SEARCH_TOPIC,
                groupId: tempGroupId,
                count: collected.length,
                filters: { query: filterQuery ?? "없음", type: filterType ?? "전체" },
                messages: collected,
              },
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
