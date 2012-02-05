from datetime import datetime, timedelta
from operator import itemgetter, attrgetter
import os

from dulwich.repo import Repo
from dulwich import objects
from dulwich.errors import NotCommitError

from pyvcs.commit import Commit
from pyvcs.exceptions import CommitDoesNotExist, FileDoesNotExist, FolderDoesNotExist
from pyvcs.repository import BaseRepository
from pyvcs.utils import generate_unified_diff


def traverse_tree(repo, tree):
    for mode, name, sha in tree.entries():
        if isinstance(repo.get_object(sha), objects.Tree):
            for item in traverse_tree(repo, repo.get_object(sha)):
                yield os.path.join(name, item)
        else:
            yield name

def get_differing_files(repo, past, current):
    past_files = {}
    current_files = {}
    if past is not None:
        past_files = dict([(name, sha) for mode, name, sha in past.entries()])
    if current is not None:
        current_files = dict([(name, sha) for mode, name, sha in current.entries()])

    added = set(current_files) - set(past_files)
    removed = set(past_files) - set(current_files)
    changed = [o for o in past_files if o in current_files and past_files[o] != current_files[o]]

    for name in added:
        sha = current_files[name]
        yield name
        if isinstance(repo.get_object(sha), objects.Tree):
            for item in get_differing_files(repo, None, repo.get_object(sha)):
                yield os.path.join(name, item)

    for name in removed:
        sha = past_files[name]
        yield name
        if isinstance(repo.get_object(sha), objects.Tree):
            for item in get_differing_files(repo, repo.get_object(sha), None):
                yield os.path.join(name, item)

    for name in changed:
        past_sha = past_files[name]
        current_sha = current_files[name]
        if isinstance(repo.get_object(past_sha), objects.Tree):
            for item in get_differing_files(repo, repo.get_object(past_sha), repo.get_object(current_sha)):
                yield os.path.join(name, item)
        else:
            yield name


class Repository(BaseRepository):
    def __init__(self, *args, **kwargs):
        super(Repository, self).__init__(*args, **kwargs)

        self._repo = Repo(self.path)

    def _get_commit(self, commit_id):
        try:
            return self._repo[commit_id]
        except Exception, e:
            raise CommitDoesNotExist("%s is not a commit" % commit_id)

    def _get_obj(self, sha):
        return self._repo.get_object(sha)

    def _diff_files(self, commit_id1, commit_id2):
        if commit_id1 == 'NULL':
            commit_id1 = None
        if commit_id2 == 'NULL':
            commit_id2 = None
        tree1 = self._get_obj(self._get_obj(commit_id1).tree) if commit_id1 else None
        tree2 = self._get_obj(self._get_obj(commit_id2).tree) if commit_id2 else None
        return sorted(get_differing_files(
            self._repo,
            tree1,
            tree2,
        ))

    def get_commit_by_id(self, commit_id):
        commit = self._get_commit(commit_id)
        parent = commit.parents[0] if len(commit.parents) else 'NULL'
        files = self._diff_files(commit.id, parent)
        return Commit(commit.id, commit.committer,
            datetime.fromtimestamp(commit.commit_time), commit.message, files,
            lambda: generate_unified_diff(self, files, parent, commit.id))

    def get_recent_commits(self, since=None):
        if since is None:
            #since = datetime.fromtimestamp(self._repo.commit(self._repo.head()).commit_time) - timedelta(days=5)
            since = datetime.fromtimestamp(self._repo[self._repo.head()].commit_time) - timedelta(days=5)
        pending_commits = self._repo.get_refs().values()#[self._repo.head()]
        history = {}
        while pending_commits:
            head = pending_commits.pop(0)
            try:
                commit = self._repo[head]
            except KeyError:
                raise CommitDoesNotExist
            if not isinstance(commit, objects.Commit) or commit.id in history or\
               datetime.fromtimestamp(commit.commit_time) <= since:
                continue
            history[commit.id] = commit
            pending_commits.extend(commit.parents)
        commits = filter(lambda o: datetime.fromtimestamp(o.commit_time) >= since, history.values())
        commits = map(lambda o: self.get_commit_by_id(o.id), commits)
        return sorted(commits, key=attrgetter('time'), reverse=True)

    def get_branch(self,revision):
        '''
        For the given revision, return branch name.
        '''
        branches_dikt = self._repo.get_refs()
        for key, value in branches_dikt.items():
            if key != 'HEAD' and value == revision:
                return key.split('/')[2]
        
    def list_directory1(self, path, revision=None):
        '''
        List all folders and files for the given path.
        '''
        branch_name = self.get_branch(revision)
        valid_path = path
        if revision is None:
            return self.get_branches()
        elif revision is 'NULL':
            return ([],[])
        else:
            commit = self._get_commit(revision)
            
        tree = self._repo[commit.tree]
        path = filter(bool, path.split(os.path.sep))
        while path:
            part = path.pop(0)
            found = False
            for mode, name, hexsha in self._repo[tree.id].entries():
                if part == name:
                    found = True
                    tree = self._repo[hexsha]
                    break
            if not found:
                raise FolderDoesNotExist
        all_files_and_folders = []

        for mode, name, hexsha in tree.entries():
            if isinstance(self._repo.get_object(hexsha), objects.Tree):
                folder_dikt = self._get_commit_details(name,valid_path,'directory',branch_name,revision)
                all_files_and_folders.append(folder_dikt)
            elif isinstance(self._repo.get_object(hexsha), objects.Blob):
                file_dikt = self._get_commit_details(name,valid_path,'file',branch_name,revision)
                all_files_and_folders.append(file_dikt)

        return all_files_and_folders
    
    def _get_commit_details(self,name,valid_path,path_type,branch,revision):
        '''
        Creates commit dictionary for file and folder, dictionary contains
        author, date, url_path, type(file/folder) branch_name. 
        '''
        full_path = valid_path+name
        try:
            commit_dict = self.get_history(full_path,revision)[0]
        except:
            pass
        path_dict = {'name':name,
                       'author':commit_dict['author'] or '',
                       'date':commit_dict['date'] or '',
                       'url_path': full_path,
                       'type':path_type,
                       'branch_name':branch
                       }
        if path_type == 'directory':
            path_dict['url_path'] = path_dict['url_path'] + '/'
        else:
            path_dict['repos_path'] = full_path
        return path_dict

    def file_contents(self, path, revision=None):
        if revision is None:
            commit = self._get_commit(self._repo.head())
        elif revision is 'NULL':
            return ''
        else:
            commit = self._get_commit(revision)
        tree = self._repo[commit.tree]
        path = path.split(os.path.sep)
        path, filename = path[:-1], path[-1]
        while path:
            part = path.pop(0)
            for mode, name, hexsha in self._repo[tree.id].entries():
                if part == name:
                    tree = self._repo[hexsha]
                    break
        for mode, name, hexsha in tree.entries():
            if name == filename:
                return self._repo[hexsha].as_pretty_string()
        raise FileDoesNotExist
    
    def get_history(self,path,branch_revision=None):
        '''
        Returns list of log messages for the given path
        '''
        if path.endswith('/'):
            path = path[:-1]        
        file_log = []
        if branch_revision is None:
            revision = self._repo.head()
        else:
            revision = branch_revision
        commits = [e.commit for e in self._repo.get_walker(include=[revision],paths=[path])]
        for commit in commits:
            dict_log = {'author':commit.committer.split(' ')[0], #returns name & email, so taking only name
                        'log':commit.message,
                        'date':datetime.fromtimestamp(commit.commit_time),
                        'revnum':commit.id}
            file_log.append(dict_log)
        return file_log    
