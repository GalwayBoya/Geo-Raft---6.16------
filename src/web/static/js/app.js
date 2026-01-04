const { createApp, ref, onMounted, nextTick, watch } = Vue;

const app = createApp({
    setup() {
        const nodes = ref([]);
        const events = ref([]);
        const nodeRefs = ref([]);
        const animationClasses = ref({});
        const ws = ref(null);
        const isolatedNodeId = ref(-1);
        const isIsolating = ref(false);
        const isInjectingFault = ref(false);
        const isSendingCommand = ref(false);
        const connectionStatus = ref('disconnected');

        watch(nodes, (newNodes, oldNodes) => {
            if (oldNodes.length === 0 || newNodes.length !== oldNodes.length) {
                return;
            }

            newNodes.forEach((newNode, index) => {
                const oldNode = oldNodes[index];
                if (newNode.role !== oldNode.role) {
                    let animClass = '';
                    if (newNode.role === 'Leader') {
                        animClass = 'flash-leader';
                    } else if (newNode.role === 'Candidate') {
                        animClass = 'flash-candidate';
                    }

                    if (animClass) {
                        animationClasses.value[newNode.id] = animClass;
                        setTimeout(() => {
                            animationClasses.value[newNode.id] = '';
                        }, 800); // Duration of the animation
                    }
                }
            });
        }, { deep: true });

        const connectWebSocket = () => {
            const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const host = window.location.host;
            ws.value = new WebSocket(`${proto}//${host}/ws`);

            ws.value.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    nodes.value = data.nodes || [];
                    events.value = data.events || [];
                    isolatedNodeId.value = data.isolatedNodeId ?? -1;
                    nextTick(() => {
                        lucide.createIcons();
                    });
                } catch (e) {
                    console.error('Error parsing message:', e);
                }
            };

            ws.value.onopen = () => {
                console.log('WebSocket connection established');
                connectionStatus.value = 'connected';
            };

            ws.value.onclose = () => {
                console.log('WebSocket connection closed. Retrying in 2 seconds...');
                connectionStatus.value = 'disconnected';
                setTimeout(connectWebSocket, 2000);
            };

            ws.value.onerror = (error) => {
                console.error('WebSocket error:', error);
                connectionStatus.value = 'disconnected';
                ws.value.close();
            };
        };

        const sendCommand = async () => {
            isSendingCommand.value = true;
            try {
                const response = await fetch('/send-command', {
                    method: 'POST',
                });
                const result = await response.json();
                ElementPlus.ElMessage({
                    message: result.message,
                    type: response.ok ? 'success' : 'warning',
                });
            } catch (error) {
                console.error('Error sending command:', error);
                ElementPlus.ElMessage({
                    message: 'Failed to send command.',
                    type: 'error',
                });
            } finally {
                isSendingCommand.value = false;
            }
        };

        const injectFault = async () => {
            isInjectingFault.value = true;
            try {
                const response = await fetch('/inject-fault', {
                    method: 'POST',
                });
                const result = await response.json();
                console.log(result.message);
                // Optionally show a notification to the user
                ElementPlus.ElMessage({
                    message: result.message,
                    type: response.ok ? 'success' : 'warning',
                });
            } catch (error) {
                console.error('Error injecting fault:', error);
                ElementPlus.ElMessage({
                    message: 'Failed to inject fault.',
                    type: 'error',
                });
            } finally {
                isInjectingFault.value = false;
            }
        };

        const toggleIsolation = async () => {
            isIsolating.value = true;
            try {
                const response = await fetch('/toggle-isolation', { method: 'POST' });
                const result = await response.json();
                 ElementPlus.ElMessage({
                    message: result.message,
                    type: response.ok ? 'success' : 'info',
                });
            } catch (error) {
                 ElementPlus.ElMessage({ message: 'Operation failed.', type: 'error' });
            } finally {
                isIsolating.value = false;
            }
        };

        const getRoleClass = (role) => {
            switch (role) {
                case 'Leader':
                    return 'leader';
                case 'Candidate':
                    return 'candidate';
                case 'Follower':
                    return 'follower';
                default:
                    return '';
            }
        };

        const formatTime = (timestamp) => {
            return new Date(timestamp).toLocaleTimeString();
        };

        const normalizeLatency = (latency) => {
            if (latency < 0) return 0;
            const maxLatency = 200; // Assume 200ms is a reasonable max for visualization
            return Math.min(100, (latency / maxLatency) * 100);
        };

        const getLatencyColor = (latency) => {
            if (latency < 0) return '#909399'; // Gray for no data
            if (latency < 50) return '#67c23a'; // Green
            if (latency < 100) return '#e6a23c'; // Yellow
            return '#f56c6c'; // Red
        };

        const formatLatency = (latency) => {
            return latency < 0 ? 'N/A' : `${latency}ms`;
        };

        onMounted(() => {
            connectWebSocket();
        });

        return {
            nodes,
            events,
            nodeRefs,
            animationClasses,
            isolatedNodeId,
            isIsolating,
            toggleIsolation,
            isInjectingFault,
            isSendingCommand,
            connectionStatus,
            getRoleClass,
            injectFault,
            sendCommand,
            formatTime,
            normalizeLatency,
            getLatencyColor,
            formatLatency,
        };
    },
});

app.use(ElementPlus);
app.mount('#app'); 