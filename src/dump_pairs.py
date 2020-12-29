from reddit_loader import *
from util import *
from stream import Stream

# ======================================================================
# Modify this section to define preprocessing.
# ----------------------------------------------------------------------

# Modify this function to decide which comments to keep.
def pre_transform_filter(comment):
    """
    An arbitrary filtering criterion. Return True if the comment should be
    kept, or False if it should be excluded.
    This filter is used before comments are transformed with the
    `comment_transformation` function below.
    """
    return not_deleted(comment) # more tests?

# Modify this function to perform other comment transformations.
def comment_transformation(comment):
    """
    An arbitrary transformation. Return the modified comment.
    """
    return wrap(trim_whitespace())(comment) # more transformations?

# Modify this function to decide which comments to keep.
def post_transform_filter(comment):
    """
    An arbitrary filtering criterion. Return True if the comment should be
    kept, or False if it should be excluded.
    This filter is used after comments have been transformed with the
    `comment_transformation` function above.
    """
    return True     # TODO: probably filter on body length?

# ======================================================================

def modify_parent_id(comment):
    """
    Change the parent ID of the comment so it matches the comment ID of
    the parent comment.
    """
    parent_id = get_parent_comment_id(comment)
    comment['parent_id'] = parent_id
    return comment

def id_pairs(stream):
    """
    The stream of comments should ideally have been processed such that
    comments that should be excluded (for whatever reason) have already been
    filtered out.
    """
    def gen():
        parents = set() # Comment IDs seen so far - potential parents.
        for comment in stream:
            comment_id = comment['id']
            parent_id = comment['parent_id']
            if parent_id in parents:
                yield (comment_id, parent_id)
            parents.add(comment_id)
    return Stream(gen())

def preprocess(stream):
    return (stream
        .filter(pre_transform_filter)
        .map(comment_transformation)
        .filter(post_transform_filter)
    )

def paired_comments_set(*files):
    """
    Returns a set of all the comment IDs of comments that are paired with
    another comment.
    """
    stream = preprocess(read_records(*files)).map(modify_parent_id)
    return id_pairs(stream).flat_map(set).to_set()

def body_pairs(stream):
    """
    Returns a stream of (comment, reply) pairs, where the elements in
    the pairs are the comment bodies.
    """
    def gen():
        id_to_body = {}
        for comment in stream:
            comment_id = comment['id']
            parent_id = comment['parent_id']
            id_to_body[comment_id] = comment['body']
            if parent_id in id_to_body:
                yield (id_to_body[parent_id], comment['body'])
    return Stream(gen())

def get_pairs(*files):
    pairs = paired_comments_set(*files)
    stream = (read_records(*files)
        .filter(lambda comment: comment['id'] in pairs)
        .map(modify_parent_id)
    )
    return body_pairs(preprocess(stream))

def dump_pairs(*files):
    for pair in get_pairs(*files):
        print(f'{pair[0]}\t{pair[1]}')

def dump_pairs_to_file(out_file, *in_files):
    out = open(out_file, mode='w')
    for pair in get_pairs(*in_files):
        out.write(f'{pair[0]}\t{pair[1]}\n')

# ======================================================================
# Run as a standalone program to dump comment pairs to a file.
# ----------------------------------------------------------------------

# The data sets to read. Modify this list to read other comment files.
data_files = [
    'RC_2006-01.bz2', 'RC_2017-11.bz2',
]

pairs_output_file = 'pairs.txt'

if __name__ == '__main__':
    dump_pairs_to_file(pairs_output_file, *data_files)
