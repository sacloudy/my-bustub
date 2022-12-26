#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  return root_page_id_ == INVALID_PAGE_ID;  // 只能这样借助这个判断啦,这个B+树也不维护其页面数量...
}

/**
 * 说一点重要的: 对一个page的使用流程(B+树怎样和bpm做交互)？
 *         答:   FetchPage(page_id)
 *            -> 根据具体类型reinterpret页面 (BPlusTreePage中有IsLeafPage方法捏)
 *            -> 读取/修改页面
 *            -> Unpin(page_id)
 */

/**
 * 查找给定key应该存放在的leaf page, 和是否真的存在无关哦。预计会在B+树的点查和插入中用到~
 * 很重要的一点是: 该函数相当于一次Fetch的(路径上的internal都被Unpin过了,最后的leaf还没有呢)
 *               也就是说在点查和插入中调用该函数后, 最后要记得Unpin!
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key) -> LeafPage * {
  Page *root = buffer_pool_manager_->FetchPage(root_page_id_);
  if (root == nullptr) {
    throw "no page can find";
  }
  // node表示暂未确定是internal page还是leaf page
  auto node = reinterpret_cast<BPlusTreePage *>(root->GetData());  // 感觉不加GetData应该也行吧,反正data_在Page的开头
  while (!node->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_id = internal->LookUp(key, comparator_);  // internal_page的ValueType直接写page_id了
    buffer_pool_manager_->UnpinPage(internal->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(child_id)->GetData());
  }
  return static_cast<LeafPage *>(node);  // 是不是编译器检测出BPlusTreePage和LeafPage的继承关系就直接给上static_cast了
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  LeafPage *leaf_page = FindLeafPage(key);
  ValueType value;
  bool exist = leaf_page->Lookup(key, value, comparator_);  // 需要传value吗
  if (exist) {
    result->emplace_back(value);
  }
  // 想想就知道FindLeafPage中Fetch比Unpin多一次, specifically, only all internal pages are fetched and unpinned.
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    std::cout << "Insert into an empty tree." << std::endl;
    StartNewTree(key, value);
    return true;
  }
  LeafPage *leaf_page = FindLeafPage(key);  // 判重不应该在FindLeafPage中进行的
  ValueType val;                            // 不关心val具体存放的值呢
  bool exist = leaf_page->Lookup(key, val, comparator_);
  if (exist) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  // 本来想着能不能直接return leaf_page->Insert()了，细想来是不行的，因为你要出来在BTree的函数中才能unpin呢，
  // 其实也行呢，就像上面Lookup那样处理呗，只不过就是把Lookup放进Insert中了:Insert语义也从无脑插入变成了返回是否插入成功了
  // 但问题就是把Lookup放进Insert中真正做插入前也还要在二分查找搜索index呢，所以还是让Insert单纯些吧:提出一个二分下标的keyIndex就好了
  leaf_page->Insert(key, value, comparator_);    // leaf_page先插入后分裂
  if (leaf_page->GetSize() == leaf_max_size_) {  // leaf_page稳定状态下最大size只能到max_size-1
    // LeafPage *new_leaf_page = dynamic_cast<LeafPage>(Split(leafPage));  // 如果非要用基类而不是模板呢
    LeafPage *new_leaf_page = Split(leaf_page);
    InsertIntoParent(leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    // 其实我感觉在这里Unpin(new_leaf_page)可能会比较好? 因为自己的事情自己做就不容易忘记
  }
  // FindLeafPage后记得Unpin, Spilt和InsertIntoParent都只是对pin住的页进行了操作
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  return true;
}

// INDEX_TEMPLATE_ARGUMENTS
// auto BPLUSTREE_TYPE::Split(BPlusTreePage *) -> BPlusTreePage * {} 用基类确实没有用模板方便,当然可能是因为我C++菜
/**
 * 很重要了, 模板类中的模板成员函数, 为的是完成类似于 用接口实现多态 的功能
 * @tparam N 本来入参想像上面一样传基类(BPlusTreePage *)但是貌似不好写的, 参考别人用的模板
 * @param full_page 传一个需要进行分裂的满的leaf/internal page
 *      对于目前的实现, 若传入leaf,其size应该是leaf的 max_size,
 *      若传入internal,其size应该是internal的 max_size + 1(别骂了,是有点危险了)
 * @return pointer to the new page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>  // B+树调用时会把N实例为LeafPage/internalPage(相当于Page中的data_部分捏)
auto BPLUSTREE_TYPE::Split(N *full_node) -> N * {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw std::string("out of memory!");
  }
  auto new_node = reinterpret_cast<N *>(new_page->GetData());  // 其实这里不加GetData应该也行,前4KB都是一样的
  new_node->Init(new_page_id, full_node->GetParentPageId(), full_node->GetMaxSize());
  // 写到这里你可能想问了:new_node一定和full_node parent_id相同吗? (假设现在站在leaf的视角,其实internal视角也一样的)
  // 大概率是, 但也不一定呢, 可能稍后其parent就分裂了, 而且正好分裂在他们之间
  // 但是这里我Init new_node的时候假设他们parent_id一样是没有问题的,
  // 因为出了Split要InsertIntoParent的,其中parent满了的话又要Split,如果需要其中的MoveHalfTo会修改这里的new_node的parent_id的

  full_node->MoveHalfTo(new_node, buffer_pool_manager_);
  // 其实是第一次真正用到泛型,还是有必要记录/说道一下的, 说两大点吧:
  // ① 按道理来说单看这么一个Split函数 因为full_node是泛型所以没法对MoveHalfTo进行任何检查的(比如参数个数)
  //    但是可能是Clion可以根据当前工程中调用Split处接受的返回值类型推断这里泛型N可能的类型:
  //        如果只有leaf/internal调用Split(注释掉另一个), 少传一个参数Clion会报错并把你引到leaf/internal定义的函数签名处
  //        如果把调用Split的地方都注释掉MoveHalfTo少传参数都不会报错
  // ② 另一个厉害的点是MoveHalfTo竟然可以直接传入T*类型的参数, 而不用把new_node先reinterpret成BPlusTreeLeafPage再传入
  //    也可以酱理解:Split中是具体类型实例化泛型参数N,而对MoveHalfTo是通过运行时传入T的具体类型来决定调用哪个具体类型的实现版本
  return new_node;
}

/**
 * 当然是下一层节点分裂了才会在上一层的parent节点中插入一条记录啦, i.e.是要在Split后调用的
 * 说个有意思的点:leaf 先Split后InsertIntoParent就把new_page的first key复制上去插入了
 *             internal 先Split会把new_page的first key隐式删除(first_key是无效的嘛),造成的效果就像是把first key推上去了
 * 你看这个函数就用的是BPlusTreePage做的抽象, 上面的Split用的就是泛型(这个得好好研究一下)
 * @param old_page 经过分裂后的full_page,后一半kv已经移到第三个参数new_page中了
 * @param key new_page中的第一元素, 就是要把他插入到parent中
 * @param new_page 存放full_page(old_page)中的后半部分kv
 * @param pTransaction
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_page, const KeyType &key, BPlusTreePage *new_page,
                                      Transaction *pTransaction) -> void {
  if (old_page->IsRootPage()) {
    page_id_t new_root_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_id);
    auto root_page = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    old_page->SetParentPageId(new_root_id);
    new_page->SetParentPageId(new_root_id);

    root_page->Init(new_root_id, INVALID_PAGE_ID, internal_max_size_);
    // 这里是HEADER_PAGE_ID的话test2的insert 4会报 AddressSanitizer: heap-buffer-overflow的(先不仔细研究了)

    // 新生成的根节点永远只有两个孩子, populate的意思是:(给文件)增添数据
    root_page->PopulateNewRoot(old_page->GetPageId(), key, new_page->GetPageId());
    root_page_id_ = new_root_id;
    UpdateRootPageId(false);  // 也不知道是要干啥, 反正根据andy提示得update一下

    buffer_pool_manager_->UnpinPage(new_root_id, true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    // buffer_pool_manager_->UnpinPage(old_page->GetPageId(), true);
    // old_page确实不用Unpin,外面会Unpin的,不过就算有一两个页忘记Unpin应该也不会过不了吧~
    return;
  }
  // 按照习惯递归终止条件要和递归逻辑空一行

  // 此时new_page的parent_id指向和old_page的一样呢(因为InsertIntoParent前都会调用Split的)
  // 那接下来就读old_page的parent进行插入呗
  page_id_t parent_id = old_page->GetParentPageId();
  Page *p_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent_page = reinterpret_cast<InternalPage *>(p_page->GetData());

  // internal_page本来应该是先分裂后插入的, 一开始我的完整做法剪切到notion笔记里了, 但是这么做有个严重的问题就是
  // 分裂逻辑还要参考待插入的值 (比如[1,3,5]要分裂,如插入的是2就只能分裂成[1]和[3,5],如果是4就只能分裂成[1,3]和[5])
  // ,这显然是复杂到不切实际的, 正确的做法应该是另找一个大一些的空间存放插入数据后的internal页面,
  // 然后再做分裂(这个空间我还以为是要一个新页并且不能要24B的header,) 经过提醒恍然大悟page放不下可以用vector呀,
  // 不过看群友internal_page先插入后分裂是能过的,虽然插入后有越界的可能, 我也就先这样吧~
  parent_page->InsertAfterPageID(old_page->GetPageId(), key, new_page->GetPageId());
  LOG_DEBUG("# Insert new_page_id=%d的第一个key into parent of %d(i.e.在page_id=%d后)", new_page->GetPageId(),
            old_page->GetPageId(), old_page->GetPageId());
  if (parent_page->GetSize() == internal_max_size_ + 1) {  // internal_page是先分裂后插入
    // 在B+Tree的函数中就没必要写parent_page->GetMaxSize()了
    LOG_DEBUG("[InsertIntoParent] 达到internal_max_size_+1啦，危险分裂");
    InternalPage *new_parent_page = Split(parent_page);
    InsertIntoParent(parent_page, new_parent_page->KeyAt(0), new_parent_page, pTransaction);
    // 注释掉先分裂后插入的逻辑，当前if外面的InsertAfterPageID移到了Split后(一个变三个啦,if外面还有一个InsertAfterPageID)
    //  if (comparator_(new_parent_page->KeyAt(0), key) > 0) { // 看,还是读这个无效的值了吧
    //    parent_page->InsertAfterPageID(old_page->GetPageId(), key, new_page->GetPageId());
    //  } else {
    //    new_parent_page->InsertAfterPageID(old_page->GetPageId(), key, new_page->GetPageId());
    //    new_page->SetParentPageId(new_parent_page->GetPageId());
    //  }
    return;
  }
  //  parent_page->InsertAfterPageID(old_page->GetPageId(), key, new_page->GetPageId());
}

/**
 * 虽然你在Insert的最前面执行, 但是因为是边界条件就把你放在最后啦
 * @param key
 * @param value
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) -> void {
  page_id_t root_page_id;
  Page *root_page = buffer_pool_manager_->NewPage(&root_page_id);
  assert(root_page != nullptr);
  auto *root = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(root_page->GetData());
  root->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);  // 一开始还以为root的parent_page是HEADER_PAGE_ID
  //  root->SetNextPageId(INVALID_PAGE_ID); 我感觉是从Init中提出来比较好,不过也无所谓
  root_page_id_ = root_page_id;
  UpdateRootPageId(true);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  std::cout << "--------------" << std::endl << "root_page_id: " << root_page_id_ << std::endl;
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",  ";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

// 难道是为了预编译类？但是这样的话也不应该注释掉就编译错误了吧
// template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
// template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
// template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
