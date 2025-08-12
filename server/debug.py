#!/usr/bin/env python3
"""
Script para debugar o estado do banco de dados
"""

import os
import sys
from pathlib import Path

# Muda para a pasta server se necessário
if os.path.basename(os.getcwd()) == 'GuessSong':
    os.chdir('server')

# Importar o db_manager
try:
    import db_manager
except ImportError:
    print("ERRO: Não foi possível importar db_manager")
    print("Verifique se você está na pasta correta e se o arquivo db_manager.py existe")
    sys.exit(1)

def debug_database():
    """Função para debugar o estado atual do banco de dados"""
    
    print("=== DEBUG DO BANCO DE DADOS ===\n")
    
    try:
        # 1. Verificar se o banco existe
        db_path = Path("music_cache.db")  # Assumindo que é esse o nome
        if db_path.exists():
            print(f"✅ Banco de dados encontrado: {db_path}")
            print(f"   Tamanho: {db_path.stat().st_size} bytes")
        else:
            print(f"❌ Banco de dados não encontrado em: {db_path}")
            
        # 2. Tentar conectar e verificar tabelas
        print("\n--- Testando Conexão ---")
        processed_ids = db_manager.get_all_processed_track_ids()
        print(f"✅ Conexão com banco OK. IDs processados: {len(processed_ids)}")
        
        # 3. Verificar status das tracks
        print("\n--- Status das Tracks ---")
        status_counts = {}
        
        # Verificar cada status possível
        statuses_to_check = ['pending', 'downloaded', 'failed', 'failed_permanent']
        
        for status in statuses_to_check:
            try:
                tracks = db_manager.get_tracks_by_status(status)
                count = len(tracks)
                status_counts[status] = count
                print(f"  {status}: {count} tracks")
                
                # Mostrar algumas tracks como exemplo
                if count > 0 and count <= 3:
                    for track in tracks:
                        print(f"    - {track.get('title', 'N/A')} por {track.get('artist', 'N/A')}")
                elif count > 3:
                    for i, track in enumerate(tracks[:3]):
                        print(f"    - {track.get('title', 'N/A')} por {track.get('artist', 'N/A')}")
                    print(f"    ... e mais {count - 3} tracks")
                        
            except Exception as e:
                print(f"  ❌ Erro ao buscar status '{status}': {e}")
        
        # 4. Verificar arquivos no diretório
        print("\n--- Arquivos no Diretório ---")
        audio_dir = Path("static/audio")
        if audio_dir.exists():
            audio_files = list(audio_dir.glob("*.webm"))
            print(f"  Arquivos .webm encontrados: {len(audio_files)}")
            
            if len(audio_files) > 0:
                total_size = sum(f.stat().st_size for f in audio_files)
                print(f"  Tamanho total: {total_size / (1024*1024):.2f} MB")
                
                # Verificar alguns arquivos
                for i, file in enumerate(audio_files[:5]):
                    size_kb = file.stat().st_size / 1024
                    print(f"    - {file.name}: {size_kb:.1f} KB")
                if len(audio_files) > 5:
                    print(f"    ... e mais {len(audio_files) - 5} arquivos")
        else:
            print(f"  ❌ Diretório de áudio não encontrado: {audio_dir}")
        
        # 5. Sugestões baseadas no que encontramos
        print("\n--- Diagnóstico ---")
        
        total_tracks = sum(status_counts.values())
        if total_tracks == 0:
            print("  🔍 Não há tracks no banco. Execute o script de cache primeiro.")
        elif status_counts.get('pending', 0) == 0:
            print("  ⚠️  Não há tracks pendentes para download.")
            if status_counts.get('downloaded', 0) > 0:
                audio_files_count = len(list(audio_dir.glob("*.webm"))) if audio_dir.exists() else 0
                if audio_files_count == 0:
                    print("  💡 Sugestão: Tracks marcadas como downloaded mas sem arquivos. Use --reset-missing")
                else:
                    print("  ✅ Tracks já foram baixadas com sucesso.")
            else:
                print("  💡 Sugestão: Execute o reset do banco ou verifique o db_manager.py")
        else:
            print(f"  ✅ {status_counts['pending']} tracks prontas para download.")
            
    except Exception as e:
        print(f"❌ Erro durante debug: {e}")
        import traceback
        traceback.print_exc()

def reset_failed_downloads():
    """Reseta downloads que falharam ou que estão marcados como downloaded mas sem arquivo"""
    
    print("\n=== RESETANDO DOWNLOADS PROBLEMÁTICOS ===\n")
    
    try:
        audio_dir = Path("static/audio")
        reset_count = 0
        
        # 1. Resetar tracks marcadas como downloaded mas sem arquivo
        downloaded_tracks = db_manager.get_tracks_by_status('downloaded')
        for track in downloaded_tracks:
            if track.get('filepath'):
                file_path = Path(track['filepath'])
                if not file_path.exists() or file_path.stat().st_size < 1000:  # menor que 1KB
                    db_manager.update_track_status(track['id'], 'pending')
                    reset_count += 1
                    print(f"  ↻ Resetado: {track['title']} - {track['artist']}")
        
        # 2. Resetar falhas permanentes para nova tentativa (opcional)
        failed_permanent = db_manager.get_tracks_by_status('failed_permanent')
        if len(failed_permanent) > 0:
            response = input(f"\nResetar {len(failed_permanent)} falhas permanentes? (s/N): ")
            if response.lower() in ['s', 'sim', 'y', 'yes']:
                for track in failed_permanent:
                    db_manager.update_track_status(track['id'], 'pending')
                    reset_count += len(failed_permanent)
                print(f"  ↻ Resetadas {len(failed_permanent)} falhas permanentes")
        
        print(f"\n✅ Total resetado: {reset_count} tracks")
        
    except Exception as e:
        print(f"❌ Erro ao resetar: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Debug do banco de dados de músicas")
    parser.add_argument('--reset-missing', action='store_true', 
                       help="Resetar tracks marcadas como downloaded mas sem arquivo")
    
    args = parser.parse_args()
    
    debug_database()
    
    if args.reset_missing:
        reset_failed_downloads()