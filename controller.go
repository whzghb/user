package main

import (
	"context"
	"fmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"time"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	userv1 "user/pkg/apis/user/v1"
	clientset "user/pkg/client/clientset/versioned"
	userscheme "user/pkg/client/clientset/versioned/scheme"
	userinformers "user/pkg/client/informers/externalversions/user/v1"
	listers "user/pkg/client/listers/user/v1"
)

const controllerAgentName = "user-controller"

const (
	SuccessSynced = "Synced"

	MessageResourceSynced = "User synced successfully"
)

// Controller is the controller implementation for User resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface
	// userclientset is a clientset for our own API group
	userclientset clientset.Interface

	usersLister listers.UserLister
	usersSynced cache.InformerSynced

	kubeInformer informers.SharedInformerFactory

	workqueue workqueue.RateLimitingInterface

	recorder record.EventRecorder
}

// NewController returns a new user controller
func NewController(
	kubeclientset kubernetes.Interface,
	userclientset clientset.Interface,
	userInformer userinformers.UserInformer,
	kubeInformer informers.SharedInformerFactory) *Controller {

	utilruntime.Must(userscheme.AddToScheme(scheme.Scheme))

	glog.Info("Creating event broadcaster")

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:    kubeclientset,
		userclientset:    userclientset,
		usersLister:      userInformer.Lister(),
		kubeInformer:     kubeInformer,
		usersSynced:      userInformer.Informer().HasSynced,
		workqueue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "User"),
		recorder:         recorder,
	}
	glog.Info("Setting up event handlers")
	// Set up an event handler for when User resources change
	userInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueUser,
		UpdateFunc: func(old, new interface{}) {
			oldUser := old.(*userv1.User)
			newUser := new.(*userv1.User)
			if oldUser.ResourceVersion == newUser.ResourceVersion || oldUser.Status.IsLogin == newUser.Status.IsLogin{
                //版本一致，就表示没有实际更新的操作，立即返回
				return
			}
			controller.enqueueUser(new)
		},
		DeleteFunc: controller.enqueueUserForDelete,
	})

	return controller
}

//在此处开始controller的业务
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	glog.Info("开始controller业务，开始一次缓存数据同步")
	if ok := cache.WaitForCacheSync(stopCh, c.usersSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("worker启动")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("worker已经启动")
	<-stopCh
	glog.Info("worker已经结束")

	return nil
}

func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// 取数据处理
func (c *Controller) processNextWorkItem() bool {

	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		defer c.workqueue.Done(obj)
		var key string
		var ok bool

		if key, ok = obj.(string); !ok {

			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// 在syncHandler中处理业务
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}

		c.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// 处理
func (c *Controller) syncHandler(key string) error {
	// 如果对象为命名空间级别
	// Convert the namespace/name string into a distinct namespace and name
	//namespace, name, err := cache.SplitMetaNamespaceKey(key)
	//if err != nil {
	//	runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
	//	return nil
	//}

	// 从缓存中取对象
	user, err := c.usersLister.Get(key)

	glog.Info(user, err)

	if err != nil {
		// 如果User对象被删除了，就会走到这里，所以应该在这里加入执行
		if errors.IsNotFound(err) {
			glog.Infof("User对象被删除，请在这里执行实际的删除业务: %s ...", key)

			return nil
		}

		runtime.HandleError(fmt.Errorf("failed to list user by: %s", key))

		return err
	}

	glog.Infof("这里是user对象的期望状态: %#v ...", user)
	glog.Infof("实际状态是从业务层面得到的，此处应该去的实际状态，与期望状态做对比，并根据差异做出响应(新增或者删除)")

	user.Status.IsLogin = "true"
	user.Status.LastLogTime = time.Now().Format("2006-01-02 15:04:05")

	_, err = c.userclientset.StableV1().Users().Update(context.TODO(), user, metav1.UpdateOptions{})
	if err != nil{
		runtime.HandleError(fmt.Errorf("failed to update user by: %s", key))
	}
	c.recorder.Event(user, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)

	nodes, _ := c.kubeInformer.Core().V1().Nodes().Lister().List(labels.Everything())
	fmt.Println(nodes)
	return nil
}

// 数据先放入缓存，再入队列
func (c *Controller) enqueueUser(obj interface{}) {
	var key string
	var err error
	// 将对象放入缓存
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	// 将key放入队列
	c.workqueue.AddRateLimited(key)
}

// 删除操作
func (c *Controller) enqueueUserForDelete(obj interface{}) {
	var key string
	var err error
	// 从缓存中删除指定对象
	key, err = cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		runtime.HandleError(err)
		return
	}
	//再将key放入队列
	c.workqueue.AddRateLimited(key)
}
