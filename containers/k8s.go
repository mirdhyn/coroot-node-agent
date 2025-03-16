package containers

import (
	"context"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

var (
	k8sClient      *kubernetes.Clientset
	k8sClientOnce  sync.Once
	podLabelsCache = make(map[string]map[string]string)
	podLabelsMutex sync.RWMutex
)

func initK8sClient() error {
	var initErr error
	k8sClientOnce.Do(func() {
		config, err := rest.InClusterConfig()
		if err != nil {
			initErr = fmt.Errorf("failed to initialize k8s client config: %v", err)
			return
		}

		client, err := kubernetes.NewForConfig(config)
		if err != nil {
			initErr = fmt.Errorf("failed to create k8s client: %v", err)
			return
		}

		k8sClient = client

		// Start background refresh
		go func() {
			for {
				refreshPodLabelsCache()
				time.Sleep(30 * time.Second)
			}
		}()
	})
	return initErr
}

func refreshPodLabelsCache() {
	if k8sClient == nil {
		return
	}

	pods, err := k8sClient.CoreV1().Pods("").List(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list pods: %v", err)
		return
	}

	newCache := make(map[string]map[string]string)
	for _, pod := range pods.Items {
		key := pod.Namespace + "/" + pod.Name
		newCache[key] = pod.Labels
		klog.V(4).Infof("Cached labels for pod %s: %v", key, pod.Labels)
	}

	podLabelsMutex.Lock()
	podLabelsCache = newCache
	podLabelsMutex.Unlock()

	klog.V(3).Infof("Updated pod labels cache, found %d pods", len(newCache))
}

func getPodLabels(namespace, podName string) map[string]string {
	// Initialize client if not already done
	initK8sClient()

	podLabelsMutex.RLock()
	defer podLabelsMutex.RUnlock()

	key := namespace + "/" + podName
	if labels, exists := podLabelsCache[key]; exists {
		klog.V(4).Infof("Found cached labels for %s: %v", key, labels)
		return labels
	}
	return nil
}
