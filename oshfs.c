#define _OSH_FS_VERSION 2018051000
#define FUSE_USE_VERSION 26
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <fuse.h>
#include <sys/mman.h>

typedef unsigned long long my_bid_t;
typedef unsigned short MODE_T;
#define mem_size ((size_t)(4 * 1024 * 1024 * (size_t)1024))
#define blocksize (64 * 1024)
#define blocknr (mem_size / blocksize)
#define mark_start 1
#define sizeof_tags (sizeof(unsigned short) + sizeof(void*) + 3 * sizeof(my_bid_t))
#define mark_blocknr ((my_bid_t)(blocknr * sizeof(struct tags) / blocksize))
#define blockdata (blocksize - sizeof(void*) - sizeof(my_bid_t))
#define SUPERBLOCK 1
#define MARKBLOCK 2
#define FILENODE 3
#define BLOCK 4
struct tags* mark_blocks;
char last_path[255];
struct filenode* last_filenode;

struct filenode {
    char filename[255];
    my_bid_t content;
    my_bid_t tail;
    struct stat st;
    my_bid_t bid;
};
struct memstate{
    my_bid_t bid;
    size_t mem_total_space;
    size_t used_space;
    struct tags* mark_blocks;
    my_bid_t free_tags;
    my_bid_t root;
    my_bid_t tail;
    //void* flag[size/(size_t)blocksize/(size_t)(blocksize * 8)];
} *mem_state;
struct tags{
    unsigned short is_free;
    void* content;
    my_bid_t bid;
    my_bid_t pre_bid;
    my_bid_t next_bid;
}*tag;
static int set_link_table(){
    printf("set link table\n");
    mem_state->free_tags = 0;
    printf("set free tag\n");
    printf("total %lld", blocknr);
    for(my_bid_t i = 0; i < blocknr; i++){
        //printf("set free tag\n");
        mem_state->mark_blocks[i].is_free = 0;
        mem_state->mark_blocks[i].content = NULL;
        mem_state->mark_blocks[i].bid = i;
        mem_state->mark_blocks[i].pre_bid = (i == 0) ? blocknr : (i - 1);
        mem_state->mark_blocks[i].next_bid = i + 1;
        //if(i % 10000 == 0 || i >= 50000) printf("set link %lld\n", i);
    }
    return 0;
}
 my_bid_t pop_queue(){
    //printf("pop_queue\n");
    struct tags* markblocks = mem_state->mark_blocks;
    my_bid_t bid = mem_state->free_tags;
    if(bid == blocknr)return blocknr;
    my_bid_t nextbid = markblocks[bid].next_bid;
    mem_state->free_tags = nextbid;
    if(nextbid != blocknr)markblocks[nextbid].pre_bid = blocknr;
    //printf("pop_queue %lld\n", bid);
    return bid;
}
static my_bid_t mark_block(my_bid_t bid, MODE_T MODE){
    printf("mark block %lld\n", bid);
    if(bid == blocknr)return blocknr;
    switch(MODE){
        case SUPERBLOCK:{
            mem_state->mark_blocks[bid].is_free = SUPERBLOCK;
            mem_state->mark_blocks[bid].content = (void*)mem_state;
            mem_state->mark_blocks[bid].pre_bid = blocknr;
            mem_state->mark_blocks[bid].next_bid = blocknr;
            mem_state->bid = bid;
            break;
        }
        case MARKBLOCK:{
            mem_state->mark_blocks[bid].is_free = MARKBLOCK;
            mem_state->mark_blocks[bid].content = ((void*)(mem_state->mark_blocks)) + (size_t)bid * blocksize;
            mem_state->mark_blocks[bid].pre_bid = bid - 1;
            mem_state->mark_blocks[bid].next_bid = bid + 1;
            mem_state->bid = bid;
            break;
        }
        case FILENODE:{
            mem_state->mark_blocks[bid].is_free = FILENODE;
            mem_state->mark_blocks[bid].content = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            mem_state->mark_blocks[bid].pre_bid = blocknr;
            mem_state->mark_blocks[bid].next_bid = mem_state->root;
            if(mem_state->root == blocknr){
                mem_state->root = bid;
                mem_state->tail = bid;
            }
            else{
                mem_state->mark_blocks[mem_state->root].pre_bid = bid;
                mem_state->root = bid;
            }
            break;
        }
        case BLOCK:{
            mark_blocks[bid].is_free = BLOCK;
            mark_blocks[bid].content = mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            mark_blocks[bid].next_bid = blocknr;
            if(last_filenode->content == blocknr){
                mark_blocks[bid].pre_bid = last_filenode->bid;
                last_filenode->content = bid;
                last_filenode->tail = bid;
            }
            else{
                mark_blocks[bid].pre_bid = last_filenode->tail;
                mark_blocks[last_filenode->tail].next_bid = bid;
                last_filenode->tail = bid;
            }
            break;
        }
        default: break;
    }
    mem_state->used_space += blocksize;
    return bid;
}
static my_bid_t rmv_block(my_bid_t bid, MODE_T MODE){
    printf("unmark block id = %lld\n", bid);
    switch(MODE){
        case FILENODE:{
            mark_blocks[bid].is_free = 0;
            if(mark_blocks[bid].pre_bid == blocknr){
                mem_state->root = mark_blocks[bid].next_bid;
            }
            else{
                mark_blocks[mark_blocks[bid].pre_bid].next_bid = mark_blocks[bid].next_bid;
            }
            if(mark_blocks[bid].next_bid == blocknr){
                mem_state->tail = mark_blocks[bid].pre_bid;
            }
            else{
                mark_blocks[mark_blocks[bid].next_bid].pre_bid = mark_blocks[bid].pre_bid;
            }

            mark_blocks[bid].next_bid = mem_state->free_tags;
            mark_blocks[bid].pre_bid = blocknr;
            mark_blocks[mem_state->free_tags].pre_bid = bid;
            mem_state->free_tags = bid;
            munmap(mark_blocks[bid].content, blocksize);
            mark_blocks[bid].content=NULL;
            break;
        }
        case BLOCK:{
            mark_blocks[bid].is_free = 0;
            mark_blocks[bid].next_bid = mem_state->free_tags;
            mark_blocks[bid].pre_bid = blocknr;
            mark_blocks[mem_state->free_tags].pre_bid = bid;
            mem_state->free_tags = bid;
            munmap(mark_blocks[bid].content, blocksize);
            mark_blocks[bid].content=NULL;
            break;
        }
    }
}
static void *oshfs_init(struct fuse_conn_info *conn){
    printf("init\n");
    mem_state = (struct memstate*)mmap(NULL, blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    mem_state->mem_total_space = mem_size;
    mem_state->used_space = 0;
    mem_state->root = blocknr;
    mem_state->mark_blocks = NULL;
    mem_state->tail = blocknr;
    mem_state->mark_blocks  = mmap(NULL, mark_blocknr * blocksize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if(mem_state->mark_blocks == NULL)printf("fail to set mark blocks\n");
    printf("   %lld    %lld\n", mem_state->mark_blocks, &(mem_state->mark_blocks[0]));
    printf("mark blocks\n");
    //memset(mem_state->mark_blocks, 0, mark_blocknr * blocksize);
    printf("set mark blocks succeed\n");
    set_link_table();
    printf("set link table succeed\n");
    mark_block(pop_queue(), SUPERBLOCK);
    printf("mark SUPERBLOCK succeed\n");
    for(my_bid_t i = 0; i < mark_blocknr; i++){
        mark_block(pop_queue(), MARKBLOCK);
    }
    mark_blocks = mem_state->mark_blocks;
    printf("init succeed\n");
}
static int create_filenode(const char *filename, const struct stat *st){
    printf("create file node\n");
    my_bid_t bid = pop_queue();
    if(bid == blocknr)return -ENOENT;
    mark_block(bid, FILENODE);
    struct filenode* new= (struct filenode*)(mem_state->mark_blocks[bid].content);
    memcpy(new->filename, filename, strlen(filename) + 1);
    memcpy(&(new->st), st, sizeof(struct stat));
    new->content = blocknr;
    new->tail = blocknr;
    new->bid = bid;
    return 0;
}
static my_bid_t get_filenode(const char* filename){
    printf("get file node\n");
    my_bid_t node = mem_state->root;
    while(node != blocknr){
        if(strcmp(((struct filenode*)(mark_blocks[node].content))->filename, filename + 1) != 0)
            node = mark_blocks[node].next_bid;
        else
        {
            printf("get file node succeed bid = %lld   ", node);
            puts(filename); printf("\n");
            return node;
        }
    }
    printf("get file node blocknr\n");
    return blocknr;
}
static int oshfs_getattr(const char *path, struct stat *stbuf){
    printf("get attr\n");
    int ret = 0;
    my_bid_t node_bid = get_filenode(path);
    struct filenode* node = NULL;
    if(node_bid != blocknr)node = (struct filenode*)(mark_blocks[node_bid].content);
    if(strcmp(path, "/") == 0){
        memset(stbuf, 0, sizeof(struct stat));
        stbuf->st_mode = S_IFDIR | 0755;
    }else if (node){
        memcpy(stbuf, &(node->st), sizeof(struct stat));
    }else {
        ret = -ENOENT;
    }
    printf("get attr succeed\n");
    return ret;
}
static int oshfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    printf("readdir\n");
    my_bid_t bid = mem_state->root;
    struct filenode* node;
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);
    while(bid != blocknr){
        node = ((struct filenode*)(mark_blocks[bid].content));
        filler(buf, node->filename, &(node->st), 0);
        bid = mark_blocks[bid].next_bid;
    }
    printf("readdir succeed\n");
    return 0;
}
static int oshfs_mknod(const char *path, mode_t mode, dev_t dev){
    printf("mknod\n");
    struct stat st;
    st.st_mode = S_IFREG | 0644;
    st.st_uid = fuse_get_context()->uid;
    st.st_gid = fuse_get_context()->gid;
    st.st_nlink = 1;
    st.st_size = 0;
    int ret = create_filenode(path + 1, &st);
    printf("mknod succeed with ret = %d\n", ret);
    return ret;
}
static int oshfs_open(const char *path, struct fuse_file_info *fi)
{
    return 0;
}
char last_path[255];
struct filenode* last_filenode;
my_bid_t block_offset;
my_bid_t block_id;
unsigned long long max = 0;
static int oshfs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    printf("write\n");
    if(!size)return 0;
    size_t size_total = size;
    my_bid_t node_bid = get_filenode(path);
    struct filenode* node;
    if(node_bid == blocknr){
        printf("not such a file\n");
        return -ENOENT;
    }else{
        node = (struct filenode*)(mark_blocks[node_bid].content);
    }
    size_t formal_size = node->st.st_size;
    if(node->st.st_size < size + offset){
        node->st.st_size = size + offset;
    }
    if(size + offset > max)max = size + offset;
    printf("max size = %lld", max);
    my_bid_t bid_offset = offset / blocksize;
    offset = offset - bid_offset * blocksize;
    printf("file size = %lld\n", node->st.st_size);
    printf("offset = %lld, size = %lld\n", offset, size);
    if(strcmp(last_path, path) != 0){
        memcpy(last_path, path, strlen(path) + 1);
        last_filenode = node;
        block_offset = 0;
        block_id = node->content;
        if(block_id == blocknr){
            printf("file is empty\n");
            block_id = mark_block(pop_queue(), BLOCK);
        }
    }
    else{
        if(block_id == node_bid){
            printf("file is empty\n");
            block_id = mark_block(pop_queue(), BLOCK);
        }
    }
    if(block_offset < bid_offset){
        for(;block_offset < bid_offset; block_offset++){
            block_id = mark_blocks[block_id].next_bid;
            if(block_id == blocknr){
                block_id = mark_block(pop_queue(), BLOCK);
            }
        }
    }
    else if(block_offset > bid_offset){
        for(;block_offset > bid_offset; block_offset--){
            block_id = mark_blocks[block_id].pre_bid;
            if(block_id = node_bid)return -ENOENT;
        }
    }
    printf("found the first block to write bid = %lld\n", block_id);
    while(size){
        if(block_id == blocknr)block_id = mark_block(pop_queue(), BLOCK);
        if(block_id == blocknr)return -ENOENT;
        if(blocksize <= size){
            memcpy(((char*)(mark_blocks[block_id].content)) + offset, buf, blocksize - offset);
            size -= blocksize - offset;
            block_id = mark_blocks[block_id].next_bid;
            block_offset++;
        }
        else{
            memcpy(((char*)(mark_blocks[block_id].content)) + offset, buf, size);
            size = 0;
        }
        printf("write onr block\n");
        offset = 0;
    }
    printf("write %lld bytes\n",size_total);
    return size_total;
}
static int oshfs_truncate(const char* path, off_t size){
    printf("truncate\n");
    my_bid_t i = 0;
    my_bid_t node_bid = get_filenode(path);
    my_bid_t bid, temp;
    struct filenode* node;
    if(node_bid == blocknr){
        printf("not such a file\n");
        return -ENOENT;
    }else{
        node = (struct filenode*)(mark_blocks[node_bid].content);
    }
    block_offset = 0;
    if(strcmp(last_path, path) != 0){
        memcpy(last_path, path, strlen(path) + 1);
    }
    last_filenode = node;
    block_id = node->content;
    node->st.st_size = size;
    block_offset = size / blocksize;
    off_t offset = size - blocksize * block_offset;
    for(i = 0; i < block_offset; i++){
        if(block_id == blocknr)return 0;
        block_id = mark_blocks[block_id].next_bid;
    }
    bid = block_id;
    if(!offset){
        if(block_offset){
             mark_blocks[mark_blocks[block_id].pre_bid].next_bid = blocknr;
             node->tail = mark_blocks[block_id].pre_bid;
        }
        else{
            node->content = blocknr;
            node->tail = blocknr;
        }
    }
    else{
        mark_blocks[block_id].next_bid = blocknr;
        node->tail = block_id;
    }
    while(bid != blocknr){
        if(offset){
            memset((char*)(mark_blocks[bid].content) + offset, 0, blockdata - offset);
            temp = mark_blocks[bid].next_bid;
            mark_blocks[bid].next_bid = blocknr;
            bid = temp;
            offset = 0;
        }
        else{
            temp = mark_blocks[bid].next_bid;
            rmv_block(bid, BLOCK);
            printf("node content = %lld, node tail = %lld\n", node->content, node->tail);
            bid = temp;
        }
    }
    memset(last_path, 0, 255);
    printf("truncate succeed\n");
    return 0;
}

static int oshfs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi){
    printf("read\n");
    if(!size)return 0;
    size_t size_total = size;
    my_bid_t node_bid = get_filenode(path);
    my_bid_t bid_offset = offset / blocksize;
    offset = offset - bid_offset * blocksize;
    struct filenode* node;
    if(node_bid == blocknr){
        printf("not such a file\n");
        return -ENOENT;
    }else{
        node = (struct filenode*)(mark_blocks[node_bid].content);
    }
    size_t formal_size = node->st.st_size;
    if(node->st.st_size < size + offset)node->st.st_size = size + offset;
    printf("offset = %lld, size = %lld\n", offset, size);
    if(strcmp(last_path, path) != 0){
        memcpy(last_path, path, strlen(path) + 1);
        last_filenode = node;
        block_offset = 0;
        block_id = node->content;
        if(block_id == blocknr){
            block_id = mark_block(pop_queue(), BLOCK);
        }
    }
    else{
        if(block_id == node_bid){
            block_id = mark_block(pop_queue(), BLOCK);
        }
    }
    if(block_offset < bid_offset){
        for(;block_offset < bid_offset; block_offset++){
            block_id = mark_blocks[block_id].next_bid;
            if(block_id == blocknr){
                block_id = mark_block(pop_queue(), BLOCK);
            }
        }
    }
    else if(block_offset > bid_offset){
        for(;block_offset > bid_offset; block_offset--){
            block_id = mark_blocks[block_id].pre_bid;
            if(block_id = node_bid)return -ENOENT;
        }
    }
    while(size){
        if(block_id == blocknr)block_id = mark_block(pop_queue(), BLOCK);
        if(block_id == blocknr)return -ENOENT;
        if(blocksize <= size){
            memcpy(buf, ((char*)(mark_blocks[block_id].content)) + offset, blocksize - offset);
            size -= blocksize - offset;
            block_id = mark_blocks[block_id].next_bid;
            block_offset++;
        }
        else{
            memcpy(buf, ((char*)(mark_blocks[block_id].content)) + offset, size);
            size = 0;
        }
        printf("write onr block\n");
        offset = 0;
    }
    printf("write %lld bytes\n",size_total);
    return size_total;
}
static int oshfs_unlink(const char *path){
    printf("unlink\n");
    my_bid_t node_bid = get_filenode(path);
    struct filenode* node = (struct filenode*)(mark_blocks[node_bid].content);
    my_bid_t block_pointer, flag;
    for(block_pointer = node->content; block_pointer != blocknr; ){
        flag = block_pointer;
        block_pointer = mark_blocks[block_pointer].next_bid;
        rmv_block(flag, BLOCK);
    }
    rmv_block(node_bid, FILENODE);
    printf("unlink succeed\n");
    return 0;
}

static const struct fuse_operations op = {
    .init = oshfs_init,
    .getattr = oshfs_getattr,
    .readdir = oshfs_readdir,
    .mknod = oshfs_mknod,
    .open = oshfs_open,
    .write = oshfs_write,
    .truncate = oshfs_truncate,
    .read = oshfs_read,
    .unlink = oshfs_unlink,
};

int main(int argc, char *argv[])
{
    return fuse_main(argc, argv, &op, NULL);
}