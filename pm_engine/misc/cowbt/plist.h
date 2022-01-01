#ifndef PMEM_LIST_H_
#define PMEM_LIST_H_

#include <vector>
#include "libpm.h"

using namespace std;

// Persistent list for storing pointers
template<typename V>
class plist {
 public:
  struct node {
    struct node* next;
    V val;
  };

  struct node** head;
  struct node** tail;
  bool activate;
  int _size = 0;

  plist()
      : head(NULL),
        tail(NULL),
        activate(false) {
    head = new (struct node*);
    tail = new (struct node*);
    (*head) = NULL;
    (*tail) = NULL;
  }

  plist(void** _head, void** _tail) {
    head = (struct node**) _head;
    tail = (struct node**) _tail;
    activate = true;
  }

  plist(void** _head, void** _tail, bool _activate) {
    head = (struct node**) _head;
    tail = (struct node**) _tail;
    activate = _activate;
  }

  ~plist() {
    clear();
  }

  friend std::ostream& operator<<(std::ostream& os, const plist& list) {
    os << "head : " << list.head << " tail : " << list.tail << "\n";
    return os;
  }

  struct node* init(V val) {
    struct node* np = new struct node;

    np->next = (*head);
    np->val = val;

    (*head) = np;
    (*tail) = np;

    if (activate)
      pmemalloc_activate(np);

    _size++;
    return np;
  }

  void push_back(V val) {
    if ((*head) == NULL) {
      init(val);
      return;
    }

    struct node* tailp = NULL;
    struct node* np = new struct node;

    // Link it in at the end of the list
    np->val = val;
    np->next = NULL;

    tailp = (*tail);
    (*tail) = np;

    if (activate)
      pmemalloc_activate(np);

    tailp->next = np;
    pmem_persist(&tailp->next, sizeof(*np), 0);

    _size++;
  }

  // Returns the absolute pointer value
  V at(const int index) const {
    struct node * np = (*head);
    unsigned int itr = 0;

    while (np != NULL) {
      if (itr == index) {
        return np->val;
      } else {
        itr++;
        np = np->next;
      }
    }

    return NULL;
  }

  struct node* find(V val, struct node** prev) {
    struct node* np = (*head);
    struct node* tmp = NULL;
    bool found = false;

    while (np != NULL) {
      if (np->val == val) {
        found = true;
        break;
      } else {
        tmp = np;
        np = np->next;
      }
    }

    if (found) {
      if (prev) {
        *prev = tmp;
      }
      return np;
    } else {
      return NULL;
    }
  }

  void update(const int index, V val) {
    struct node * np = (*head);
    unsigned int itr = 0;

    while (np != NULL) {
      if (itr == index) {
        np->val = val;
        break;
      } else {
        itr++;
        np = np->next;
      }
    }
  }

  bool erase(V val) {
    struct node* prev = NULL;
    struct node* np = NULL;

    if ((*head) == NULL) {
      return false;
    }

    np = find(val, &prev);

    if (np == NULL) {
      return -1;
    } else {
      if (prev != NULL) {
        prev->next = np->next;
        pmem_persist(&prev->next, sizeof(*np), 0);
      }

      // Update head and tail
      if (np == (*head)) {
        (*head) = np->next;
      } else if (np == (*tail)) {
        (*tail) = prev;
      }

      _size--;
    }

    delete np;

    return true;
  }

  void display(void) {
    struct node* np = (*head);

    if (np == NULL) {
      cerr << "empty list" << endl;
      return;
    }

    while (np) {
      cerr << np->val;
      np = np->next;
    }
    cerr << endl;

  }

  void clear(void) {
    struct node* np = (*head);
    struct node* prev = NULL;

    while (np) {
      prev = np;
      np = np->next;
      pmemalloc_free(prev);
    }

    (*head) = NULL;
    (*tail) = NULL;
  }

  vector<V> get_data(void) {
    struct node* np = (*head);
    vector<V> data;

    while (np) {
      data.push_back(np->val);
      np = np->next;
    }

    return data;
  }

  bool empty(){
    return (_size == 0);
  }

  int size(){
    return _size;
  }


};

#endif /* PMEM_LIST_H_ */
